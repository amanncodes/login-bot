"""
OpenTelemetry Telemetry for AWS Lambda

This module provides OpenTelemetry instrumentation for AWS Lambda functions.
It's designed to be lightweight and minimal, using the same patterns as the Django implementation.

Usage:
    # At the top of your Lambda handler file:
    from lambda_telemetry import setup_lambda_telemetry, traced, traced_method, add_span_attributes
    
    # Initialize telemetry (call once at module level)
    setup_lambda_telemetry()
    
    # Use decorators to trace functions:
    @traced("scraper.fetch_comments")
    def scrape_comments(post_url: str):
        # Your code here
        pass
    
    # Add custom attributes during execution:
    add_span_attributes(post_url=post_url, comment_count=len(comments))
"""

import os
import json
import logging
from functools import wraps
from typing import Optional, Dict, Any, Callable, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.urllib import URLLibInstrumentor
from opentelemetry.propagate import set_global_textmap, get_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.context import attach, detach
from opentelemetry import context as context_api

# Logger for this module
logger = logging.getLogger(__name__)

# Global tracer instance
tracer: Optional[trace.Tracer] = None

# Flag to ensure setup is called only once
_telemetry_initialized = False


def setup_lambda_telemetry(
    service_name: str = "instagram-lambda-scraper",
    service_version: str = "1.0.0",
    otlp_endpoint: Optional[str] = None,
    otlp_headers: Optional[Dict[str, str]] = None,
) -> trace.Tracer:
    """
    Set up OpenTelemetry instrumentation for AWS Lambda.
    
    This function configures:
    - Trace provider with OTLP exporter (BetterStack)
    - Composite propagator for trace context propagation
    - Auto-instrumentation for HTTP libraries (requests, urllib)
    
    Args:
        service_name: Name of the service for telemetry
        service_version: Version of the service
        otlp_endpoint: OTLP endpoint URL (defaults to env var or BetterStack)
        otlp_headers: Headers for OTLP exporter (defaults to env var)
    
    Returns:
        Configured Tracer instance
    
    Environment Variables:
        OTLP_ENDPOINT: OTLP endpoint URL (default: BetterStack)
        BETTERSTACK_SOURCE_TOKEN: BetterStack API token
        OTLP_HEADERS: Alternative headers format (key1=value1,key2=value2)
        ENVIRONMENT: Deployment environment (default: production)
    """
    global tracer, _telemetry_initialized
    
    if _telemetry_initialized:
        logger.warning("Telemetry already initialized. Skipping setup.")
        return tracer
    
    # Get OTLP configuration from environment if not provided
    if otlp_endpoint is None:
        otlp_endpoint = os.getenv("OTLP_ENDPOINT", "https://in-otel.logs.betterstack.com/v1/traces")
    
    if otlp_headers is None:
        betterstack_token = os.getenv("BETTERSTACK_SOURCE_TOKEN", "")
        if betterstack_token:
            otlp_headers = {"Authorization": f"Bearer {betterstack_token}"}
        else:
            # Try generic OTLP_HEADERS format: "key1=value1,key2=value2"
            headers_str = os.getenv("OTLP_HEADERS", "")
            otlp_headers = {}
            if headers_str:
                for pair in headers_str.split(","):
                    if "=" in pair:
                        key, value = pair.split("=", 1)
                        otlp_headers[key.strip()] = value.strip()
    
    logger.info(f"Setting up OpenTelemetry for service: {service_name}")
    logger.info(f"OTLP Endpoint: {otlp_endpoint}")
    
    # Configure resource with service information
    resource = Resource.create({
        SERVICE_NAME: service_name,
        SERVICE_VERSION: service_version,
        "deployment.environment": os.getenv("ENVIRONMENT", "production"),
        "cloud.provider": "aws",
        "faas.name": os.getenv("AWS_LAMBDA_FUNCTION_NAME", "unknown"),
        "faas.version": os.getenv("AWS_LAMBDA_FUNCTION_VERSION", "unknown"),
    })
    
    # Create OTLP exporter
    otlp_exporter = OTLPSpanExporter(
        endpoint=otlp_endpoint,
        headers=otlp_headers or {},
    )
    
    # Set up tracer provider with batch span processor
    tracer_provider = TracerProvider(resource=resource)
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)
    
    # Set global tracer provider
    trace.set_tracer_provider(tracer_provider)
    
    # Configure composite propagator to support multiple trace context formats
    set_global_textmap(
        CompositePropagator([
            TraceContextTextMapPropagator(),  # W3C Trace Context (traceparent header)
            B3MultiFormat(),                   # B3 format (for compatibility)
        ])
    )
    
    # Get tracer instance
    tracer = trace.get_tracer(__name__)
    
    # Auto-instrument HTTP libraries
    logger.info("Enabling requests instrumentation")
    RequestsInstrumentor().instrument()
    
    logger.info("Enabling urllib instrumentation")
    URLLibInstrumentor().instrument()
    
    _telemetry_initialized = True
    logger.info("OpenTelemetry setup complete")
    
    return tracer


def get_tracer() -> trace.Tracer:
    """
    Get the global tracer instance.
    
    Returns:
        Configured Tracer instance
    
    Raises:
        RuntimeError: If telemetry has not been initialized
    """
    global tracer
    if tracer is None:
        # Auto-initialize with defaults if not already done
        logger.warning("Telemetry not initialized. Auto-initializing with defaults.")
        return setup_lambda_telemetry()
    return tracer


def add_span_attributes(**attributes: Any) -> None:
    """
    Add attributes to the current active span.
    
    Args:
        **attributes: Key-value pairs to add as span attributes
    
    Example:
        add_span_attributes(
            post_url="https://instagram.com/...",
            comment_count=150,
            retry_count=0
        )
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, value)


def add_span_event(name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
    """
    Add an event to the current active span.
    
    Args:
        name: Name of the event
        attributes: Optional attributes for the event
    
    Example:
        add_span_event("retry_triggered", {"retry_count": 1, "reason": "threshold_not_met"})
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        span.add_event(name, attributes or {})


def set_span_error(error: Exception) -> None:
    """
    Mark the current span as failed and record the exception.
    
    Args:
        error: The exception that occurred
    
    Example:
        try:
            risky_operation()
        except Exception as e:
            set_span_error(e)
            raise
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        span.record_exception(error)
        span.set_status(trace.Status(trace.StatusCode.ERROR, str(error)))


def extract_trace_context(headers: Dict[str, str]) -> Any:
    """
    Extract trace context from HTTP headers and return the context.
    
    This function reads the traceparent header (and other trace headers) from incoming
    requests and extracts the trace context. This allows you to continue a trace
    started by an upstream service.
    
    Args:
        headers: Dictionary of HTTP headers (case-insensitive)
    
    Returns:
        Extracted context that can be attached to continue the trace
    
    Example:
        # In Lambda handler
        headers = event.get('headers', {})
        ctx = extract_trace_context(headers)
        if ctx:
            token = attach(ctx)
            try:
                # Your code here - spans will be children of parent trace
                pass
            finally:
                detach(token)
    """
    from opentelemetry.trace.span import NonRecordingSpan, SpanContext
    from opentelemetry.trace import TraceFlags
    
    propagator = get_global_textmap()
    
    # Create a carrier dict with lowercase keys for consistency
    carrier = {k.lower(): v for k, v in headers.items()}
    
    logger.debug(f"Extracting trace context from carrier keys: {list(carrier.keys())}")
    
    # Extract context from carrier
    extracted_context = propagator.extract(carrier=carrier)
    
    # Get the span from the extracted context
    span = trace.get_current_span(extracted_context)
    
    # Check if we have a valid remote span context
    if span and span.get_span_context().is_valid:
        span_context = span.get_span_context()
        logger.info(f"✓ Extracted valid parent trace: {span_context.trace_id:032x}, span: {span_context.span_id:016x}")
        return extracted_context
    else:
        logger.warning("⚠ Failed to extract valid parent trace context from headers")
        logger.debug(f"Available headers: {list(carrier.keys())}")
        # Return current context if extraction failed
        return context_api.get_current()


def inject_trace_context(headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Inject current trace context into HTTP headers.
    
    This function takes the current trace context and injects it into a dictionary
    of HTTP headers. These headers can then be passed to outgoing HTTP requests
    to propagate the trace.
    
    Args:
        headers: Optional existing headers dictionary to inject into
    
    Returns:
        Dictionary with trace context headers injected
    
    Example:
        # Before making an HTTP request
        headers = inject_trace_context({"Content-Type": "application/json"})
        response = requests.post(url, headers=headers, json=payload)
    """
    if headers is None:
        headers = {}
    
    propagator = get_global_textmap()
    propagator.inject(carrier=headers)
    
    # Log what we injected for debugging
    if 'traceparent' in headers:
        logger.debug(f"Injected traceparent: {headers['traceparent']}")
    else:
        logger.warning("inject_trace_context did not inject traceparent!")
    
    return headers


def traced_http_request(
    method: str,
    url: str,
    span_name: Optional[str] = None,
    **request_kwargs
) -> Any:
    """
    Make an HTTP request with automatic tracing and error handling.
    Uses urllib.request under the hood with proper span creation.
    
    This ensures HTTP requests are properly linked to the current trace context.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        span_name: Optional custom span name
        **request_kwargs: Additional arguments passed to urllib.request.Request
    
    Returns:
        HTTP response object from urllib.request.urlopen
    
    Example:
        response = traced_http_request(
            'POST',
            'https://api.example.com/endpoint',
            data=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
    """
    from urllib import request
    from urllib.parse import urlparse
    
    # Parse URL for span attributes
    parsed = urlparse(url)
    name = span_name or f"{method} {parsed.netloc}{parsed.path}"
    
    tracer_instance = get_tracer()
    with tracer_instance.start_as_current_span(name, kind=trace.SpanKind.CLIENT) as span:
        # Set HTTP attributes
        span.set_attribute("http.method", method)
        span.set_attribute("http.url", url)
        span.set_attribute("http.scheme", parsed.scheme)
        span.set_attribute("http.host", parsed.netloc)
        span.set_attribute("http.target", parsed.path)
        
        try:
            # Make the request
            req = request.Request(url, method=method, **request_kwargs)
            response = request.urlopen(req)
            
            # Record response status
            span.set_attribute("http.status_code", response.status)
            
            # Mark as success
            if 200 <= response.status < 300:
                span.set_status(trace.Status(trace.StatusCode.OK))
            else:
                span.set_status(trace.Status(trace.StatusCode.ERROR, f"HTTP {response.status}"))
            
            return response
            
        except Exception as e:
            # Record exception details
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            
            # Add error attributes
            span.set_attribute("error", True)
            span.set_attribute("error.type", type(e).__name__)
            span.set_attribute("error.message", str(e))
            
            logger.error(f"HTTP request failed: {method} {url} - {e}")
            raise


# Type variable for generic function decorator
F = TypeVar('F', bound=Callable[..., Any])


def traced(
    span_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
) -> Callable[[F], F]:
    """
    Decorator to automatically create a span for a function.
    
    Args:
        span_name: Optional custom name for the span (defaults to function name)
        attributes: Optional dictionary of attributes to set on the span
        kind: Type of span (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER)
    
    Returns:
        Decorated function
    
    Example:
        @traced("instagram.fetch_comments", {"platform": "instagram"})
        def scrape_comments(post_url: str):
            return comments
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Use provided span name or fall back to function name
            name = span_name or f"{func.__module__}.{func.__name__}"
            
            tracer_instance = get_tracer()
            with tracer_instance.start_as_current_span(name, kind=kind) as span:
                # Set attributes if provided
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                
                # Add function metadata
                span.set_attribute("code.function", func.__name__)
                span.set_attribute("code.namespace", func.__module__)
                
                try:
                    logger.debug(f"[TRACE] Starting span: {name}")
                    result = func(*args, **kwargs)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    logger.debug(f"[TRACE] Completed span: {name}")
                    return result
                except Exception as e:
                    # Record exception with enhanced details
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    
                    # Add error attributes
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    
                    logger.error(f"[TRACE] Error in span {name}: {type(e).__name__}: {e}")
                    raise
        
        return wrapper
    return decorator


def traced_method(
    span_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
) -> Callable[[F], F]:
    """
    Decorator for class methods that automatically creates a span.
    
    Similar to @traced but specifically designed for class methods.
    Automatically adds class name to span attributes.
    
    Args:
        span_name: Optional custom name for the span (defaults to ClassName.method_name)
        attributes: Optional dictionary of attributes to set on the span
    
    Returns:
        Decorated method
    
    Example:
        class InstagramCommentScraper:
            @traced_method("instagram.scrape_comments")
            def scrape_comments(self, post_url: str):
                return comments
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Build span name with class name
            class_name = self.__class__.__name__
            name = span_name or f"{class_name}.{func.__name__}"
            
            tracer_instance = get_tracer()
            with tracer_instance.start_as_current_span(name) as span:
                # Set attributes if provided
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                
                # Add method metadata
                span.set_attribute("code.function", func.__name__)
                span.set_attribute("code.namespace", func.__module__)
                span.set_attribute("code.class", class_name)
                
                try:
                    logger.debug(f"[TRACE] Starting method span: {name}")
                    result = func(self, *args, **kwargs)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    logger.debug(f"[TRACE] Completed method span: {name}")
                    return result
                except Exception as e:
                    # Record exception with enhanced details
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    
                    # Add error attributes
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    
                    logger.error(f"[TRACE] Error in method span {name}: {type(e).__name__}: {e}")
                    raise
        
        return wrapper
    return decorator


def traced_lambda_handler(
    span_name: str = "lambda.handler",
    extract_from_event: bool = True,
) -> Callable[[F], F]:
    """
    Decorator specifically for AWS Lambda handlers that automatically:
    1. Extracts trace context from event headers
    2. Links to parent trace (e.g., from Django worker)
    3. Creates a SERVER span for the Lambda invocation
    4. Handles cleanup automatically
    
    This is a clean, decorator-only approach for Lambda functions.
    
    Args:
        span_name: Name for the Lambda handler span
        extract_from_event: Whether to extract parent trace from event headers
    
    Returns:
        Decorated Lambda handler function
    
    Example:
        @traced_lambda_handler()
        def lambda_handler(event, context):
            # Your code here - automatically linked to parent trace
            return response
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(event, context):
            # Extract trace context from Lambda event
            # Lambda Function URL format: event['headers'] contains HTTP headers
            # Direct invocation format: headers might be at top level
            headers = {}
            
            # Try event['headers'] first (Lambda URL)
            if 'headers' in event and isinstance(event.get('headers'), dict):
                headers = event['headers']
                logger.info(f"Found headers in event['headers']: {list(headers.keys())}")
                # Log traceparent if present
                traceparent = headers.get('traceparent') or headers.get('Traceparent')
                if traceparent:
                    logger.info(f"Received traceparent: {traceparent}")
                else:
                    logger.warning("No traceparent found in event headers!")
            
            # If no headers found and we have a body, check if it's direct invocation
            if not headers and isinstance(event, dict):
                # Check for trace headers at top level (direct invocation)
                trace_keys = [k for k in event.keys() if k.lower().startswith(('traceparent', 'tracestate', 'b3'))]
                if trace_keys:
                    headers = {k: event[k] for k in trace_keys}
                    logger.info(f"Found trace headers at top level: {trace_keys}")
            
            # Extract parent trace context from headers
            parent_context = context_api.get_current()  # Start with current context
            
            if extract_from_event and headers:
                parent_context = extract_trace_context(headers)
                logger.info(f"Using extracted parent context from headers")
            else:
                logger.warning("⚠ No headers found in event to extract trace context")
                logger.debug(f"Event keys: {list(event.keys()) if isinstance(event, dict) else 'not a dict'}")
            
            # CRITICAL: We need to use context_api.attach() to set the parent context
            # as the current context, then create the span within that context
            token = context_api.attach(parent_context)
            
            try:
                # Create SERVER span for Lambda handler as a child of the parent trace
                # Now the span will be created with parent_context as its parent
                tracer_instance = get_tracer()
                
                # Start span - it will automatically use the attached context as parent
                with tracer_instance.start_as_current_span(
                    span_name, 
                    kind=trace.SpanKind.SERVER
                ) as span:
                    # Log the trace linkage
                    span_ctx = span.get_span_context()
                    logger.info(f"✓ Lambda span created - Trace: {span_ctx.trace_id:032x}, Span: {span_ctx.span_id:016x}")
                    
                    # Get parent span info from the attached context for debugging
                    parent_span = trace.get_current_span(parent_context)
                    if parent_span and parent_span.get_span_context().is_valid:
                        parent_ctx = parent_span.get_span_context()
                        logger.info(f"✓ Parent span from context: {parent_ctx.span_id:016x}")
                        # Verify the link by checking if our span's parent matches
                        logger.info(f"✓ This span should be child of: {parent_ctx.span_id:016x}")
                    else:
                        logger.warning("⚠ No valid parent span found in context")
                # Add Lambda metadata
                span.set_attribute("code.function", func.__name__)
                span.set_attribute("code.namespace", func.__module__)
                span.set_attribute("faas.execution", getattr(context, 'request_id', 'unknown'))
                span.set_attribute("faas.trigger", "http")
                
                # Add event metadata for debugging
                if 'job_id' in event:
                    span.set_attribute("job.id", event['job_id'])
                if 'post_url' in event:
                    span.set_attribute("post.url", event['post_url'])
                if 'retry_count' in event:
                    span.set_attribute("retry.count", event.get('retry_count', 0))
                
                logger.info(f"[TRACE] Starting Lambda handler: {span_name}")
                logger.info(f"[TRACE] Request ID: {getattr(context, 'request_id', 'unknown')}")
                
                try:
                    result = func(event, context)
                    span.set_status(trace.Status(trace.StatusCode.OK))
                    logger.info(f"[TRACE] Lambda handler completed successfully")
                    return result
                except Exception as e:
                    # Record exception with enhanced details
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    
                    # Add error attributes
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    
                    logger.error(f"[TRACE] Lambda handler error: {type(e).__name__}: {e}")
                    raise
            finally:
                # Detach the parent context to restore previous context
                context_api.detach(token)
                logger.debug("[TRACE] Detached parent trace context")
        
        return wrapper
    return decorator


# Export commonly used items
__all__ = [
    "setup_lambda_telemetry",
    "get_tracer",
    "traced",
    "traced_method",
    "traced_lambda_handler",
    "add_span_attributes",
    "add_span_event",
    "set_span_error",
    "extract_trace_context",
    "inject_trace_context",
    "traced_http_request",
]
