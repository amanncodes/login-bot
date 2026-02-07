"""
OpenTelemetry Logger Service for BetterStack Integration

This module provides a comprehensive, modular logging and tracing solution using OpenTelemetry
with BetterStack as the backend. It handles:

1. Trace Context Propagation: Extracts traceparent headers from incoming requests
2. Trace Injection: Injects trace context into outgoing HTTP calls (boto3, requests, etc.)
3. Span Management: Context managers AND decorators for creating spans
4. Auto-instrumentation: Django, Celery, botocore/boto3 requests

Usage:
    # In Django settings.py:
    from bots.services.logger import setup_telemetry
    setup_telemetry()
    
    # In your code - Using decorators (cleaner):
    from bots.services.logger import traced, traced_method
    
    @traced("my_operation", {"key": "value"})
    def my_function():
        # Your code here
        pass
    
    # Or using context managers:
    from bots.services.logger import create_span
    
    with create_span("my_operation", {"key": "value"}):
        # Your code here
        pass
    
    # Get current trace context for manual propagation
    from bots.services.logger import get_current_trace_context
    trace_context = get_current_trace_context()
"""

import os
import logging
from contextlib import contextmanager
from functools import wraps
from typing import Optional, Dict, Any, Callable, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.propagate import set_global_textmap, get_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.propagators.composite import CompositePropagator

# Logs SDK imports
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry._logs import SeverityNumber

# Logger for this module
logger = logging.getLogger(__name__)

# Global tracer instance
tracer: Optional[trace.Tracer] = None

# Global logger provider instance
logger_provider: Optional[Any] = None

# Flag to ensure setup is called only once
_telemetry_initialized = False


def setup_telemetry(
    service_name: str = "login-bot",
    service_version: str = "0.1.0",
    otlp_endpoint: Optional[str] = None,
    otlp_headers: Optional[Dict[str, str]] = None,
    enable_django: bool = True,
    enable_botocore: bool = True,
    enable_requests: bool = True,
    enable_celery: bool = True,
) -> trace.Tracer:
    """
    Set up OpenTelemetry instrumentation with BetterStack.
    
    This function should be called once during application startup (typically in Django settings.py).
    It configures:
    - Trace provider with BetterStack OTLP exporter
    - Composite propagator (W3C Trace Context + B3) for trace propagation
    - Auto-instrumentation for Django, botocore, requests, and Celery
    
    Args:
        service_name: Name of the service for telemetry
        service_version: Version of the service
        otlp_endpoint: BetterStack OTLP endpoint URL (falls back to env var OTLP_ENDPOINT)
        otlp_headers: Headers for OTLP exporter (falls back to env var OTLP_HEADERS)
        enable_django: Whether to enable Django instrumentation
        enable_botocore: Whether to enable Botocore/boto3 instrumentation
        enable_requests: Whether to enable requests library instrumentation
        enable_celery: Whether to enable Celery instrumentation
    
    Returns:
        Configured Tracer instance
    
    Example:
        # In settings.py
        from bots.services.logger import setup_telemetry
        
        setup_telemetry(
            service_name="login-bot",
            otlp_endpoint=os.getenv("BETTERSTACK_OTLP_ENDPOINT"),
            otlp_headers={"Authorization": f"Bearer {os.getenv('BETTERSTACK_TOKEN')}"}
        )
    """
    global tracer, logger_provider, _telemetry_initialized
    
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
        "deployment.environment": os.getenv("ENVIRONMENT", "development"),
    })
    
    # Create OTLP exporter for BetterStack
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
    # This ensures we can read traceparent headers from various sources
    set_global_textmap(
        CompositePropagator([
            TraceContextTextMapPropagator(),  # W3C Trace Context (traceparent header)
            B3MultiFormat(),                   # B3 format (for compatibility)
        ])
    )
    
    # Get tracer instance
    tracer = trace.get_tracer(__name__)
    
    # Set up logs provider with OTLP exporter
    logs_endpoint = otlp_endpoint.replace("/traces", "/logs")
    logger.info(f"OTLP Logs Endpoint: {logs_endpoint}")
    
    otlp_log_exporter = OTLPLogExporter(
        endpoint=logs_endpoint,
        headers=otlp_headers or {},
    )
    
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
    set_logger_provider(logger_provider)
    
    logger.info("OpenTelemetry logs provider configured")
    
    # Auto-instrument libraries
    if enable_django:
        logger.info("Enabling Django instrumentation")
        DjangoInstrumentor().instrument()
    
    if enable_botocore:
        logger.info("Enabling Botocore/boto3 instrumentation")
        BotocoreInstrumentor().instrument()
    
    if enable_requests:
        logger.info("Enabling requests instrumentation")
        RequestsInstrumentor().instrument()
    
    if enable_celery:
        logger.info("Enabling Celery instrumentation")
        CeleryInstrumentor().instrument()
    
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
        raise RuntimeError(
            "Telemetry not initialized. Call setup_telemetry() first."
        )
    return tracer


@contextmanager
def create_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
):
    """
    Context manager for creating a span.
    
    This is a convenience wrapper that automatically starts and ends spans,
    handles exceptions, and sets attributes.
    
    Args:
        name: Name of the span
        attributes: Optional dictionary of attributes to set on the span
        kind: Type of span (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER)
    
    Yields:
        The created Span object
    
    Example:
        with create_span("database_query", {"query": "SELECT * FROM users"}):
            # Your code here
            results = db.execute(query)
    """
    tracer_instance = get_tracer()
    
    with tracer_instance.start_as_current_span(name, kind=kind) as span:
        # Set attributes if provided
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        try:
            yield span
        except Exception as e:
            # Record exception in span
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise


def extract_trace_context(headers: Dict[str, str]) -> Optional[trace.SpanContext]:
    """
    Extract trace context from HTTP headers.
    
    This function reads the traceparent header (and other trace headers) from incoming
    requests and extracts the trace context. This allows you to continue a trace
    started by an upstream service.
    
    Args:
        headers: Dictionary of HTTP headers (case-insensitive)
    
    Returns:
        Extracted SpanContext or None if no trace context found
    
    Example:
        # In a Django view
        def my_view(request):
            trace_context = extract_trace_context(dict(request.headers))
            # trace_context can now be used to create child spans
    """
    propagator = get_global_textmap()
    
    # Create a carrier dict with lowercase keys for consistency
    carrier = {k.lower(): v for k, v in headers.items()}
    
    # Extract context
    context = propagator.extract(carrier=carrier)
    
    # Get span context from the extracted context
    span = trace.get_current_span(context)
    return span.get_span_context() if span else None


def inject_trace_context(headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Inject current trace context into HTTP headers.
    
    This function takes the current trace context and injects it into a dictionary
    of HTTP headers. These headers can then be passed to outgoing HTTP requests
    (e.g., boto3 Lambda invocations, requests calls) to propagate the trace.
    
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
    
    return headers


def get_current_trace_context() -> Dict[str, str]:
    """
    Get the current trace context as a dictionary.
    
    This is useful for logging or manual propagation of trace context.
    
    Returns:
        Dictionary containing trace context (traceparent, tracestate, etc.)
    
    Example:
        trace_info = get_current_trace_context()
        logger.info(f"Current trace: {trace_info.get('traceparent', 'none')}")
    """
    return inject_trace_context({})


def add_span_attributes(**attributes: Any) -> None:
    """
    Add attributes to the current active span.
    
    Args:
        **attributes: Key-value pairs to add as span attributes
    
    Example:
        add_span_attributes(
            user_id=123,
            platform="instagram",
            cookie_id=456
        )
    """
    span = trace.get_current_span()
    if span and span.is_recording():
        for key, value in attributes.items():
            span.set_attribute(key, value)


def add_span_event(name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
    """
    Add an event to the current active span.
    
    Events are useful for marking significant moments during span execution.
    
    Args:
        name: Name of the event
        attributes: Optional attributes for the event
    
    Example:
        add_span_event("cookie_allocated", {"cookie_id": 123, "platform": "instagram"})
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


def log_with_trace(
    level: int,
    message: str,
    **attributes: Any
) -> None:
    """
    Send a structured log with automatic trace correlation.
    Logs are sent to Better Stack and automatically linked to the current trace/span.
    Also adds an event to the current span for better trace visibility.
    
    Args:
        level: Logging level (logging.INFO, logging.ERROR, etc.)
        message: Log message
        **attributes: Additional structured attributes for the log
    
    Example:
        log_with_trace(logging.INFO, "Processing comment", comment_id=123, user="john")
        log_with_trace(logging.ERROR, "API failed", error_code=500, retry_count=3)
    """
    # Get the standard logger
    log = logging.getLogger(__name__)
    
    # Add span event for trace visibility
    span = trace.get_current_span()
    if span and span.is_recording():
        event_attrs = {"log.level": logging.getLevelName(level), "log.message": message}
        event_attrs.update(attributes)
        span.add_event(f"log.{logging.getLevelName(level).lower()}", event_attrs)
        
        # Add trace context to attributes for correlation
        span_context = span.get_span_context()
        if span_context.is_valid:
            attributes['trace_id'] = f"{span_context.trace_id:032x}"
            attributes['span_id'] = f"{span_context.span_id:016x}"
    
    # Create structured log message
    if attributes:
        attr_str = " ".join(f"{k}={v}" for k, v in attributes.items())
        full_message = f"{message} | {attr_str}"
    else:
        full_message = message
    
    # Send log
    log.log(level, full_message, extra=attributes)


def log_info(message: str, **attributes: Any) -> None:
    """
    Log an info message with trace correlation and span event.
    
    Args:
        message: Log message
        **attributes: Additional structured attributes
    
    Example:
        log_info("Started processing", job_id="123", post_url="...")
    """
    log_with_trace(logging.INFO, message, **attributes)


def log_error(message: str, error: Optional[Exception] = None, **attributes: Any) -> None:
    """
    Log an error message with trace correlation, span event, and exception details.
    
    Args:
        message: Error message
        error: Optional exception object
        **attributes: Additional structured attributes
    
    Example:
        log_error("API request failed", error=e, status_code=500, retry=3)
    """
    if error:
        attributes['error_type'] = type(error).__name__
        attributes['error_message'] = str(error)
        
        # Record exception in span
        span = trace.get_current_span()
        if span and span.is_recording():
            span.record_exception(error)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(error)))
    
    log_with_trace(logging.ERROR, message, **attributes)


def log_warning(message: str, **attributes: Any) -> None:
    """
    Log a warning message with trace correlation and span event.
    
    Args:
        message: Warning message
        **attributes: Additional structured attributes
    
    Example:
        log_warning("Retry threshold reached", retry_count=5, max_retries=5)
    """
    log_with_trace(logging.WARNING, message, **attributes)


def log_debug(message: str, **attributes: Any) -> None:
    """
    Log a debug message with trace correlation and span event.
    
    Args:
        message: Debug message
        **attributes: Additional structured attributes
    
    Example:
        log_debug("Cache hit", key="user:123", ttl=3600)
    """
    log_with_trace(logging.DEBUG, message, **attributes)


# Type variable for generic function decorator
F = TypeVar('F', bound=Callable[..., Any])


def traced(
    span_name: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
) -> Callable[[F], F]:
    """
    Decorator to automatically create a span for a function.
    
    This decorator wraps a function and creates a span that tracks its execution.
    The span name defaults to the function name if not provided.
    
    Args:
        span_name: Optional custom name for the span (defaults to function name)
        attributes: Optional dictionary of attributes to set on the span
        kind: Type of span (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER)
    
    Returns:
        Decorated function
    
    Example:
        @traced("database.fetch_user", {"db.table": "users"})
        def get_user(user_id: int):
            return User.objects.get(id=user_id)
        
        # Or use default function name
        @traced()
        def process_payment(amount: float):
            return payment_service.charge(amount)
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
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    # Record exception in span
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
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
        class UserService:
            @traced_method("user_service.create_user")
            def create_user(self, username: str):
                return User.objects.create(username=username)
            
            @traced_method()  # Uses default name
            def delete_user(self, user_id: int):
                User.objects.filter(id=user_id).delete()
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
                    result = func(self, *args, **kwargs)
                    return result
                except Exception as e:
                    # Record exception in span
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    raise
        
        return wrapper
    return decorator


# Convenience logger that includes trace context
class TraceLogger:
    """
    Logger wrapper that automatically includes trace context in log messages.
    
    Example:
        trace_logger = TraceLogger("my_module")
        trace_logger.info("Processing request", user_id=123)
    """
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
    
    def _log_with_trace(self, level: int, msg: str, **kwargs):
        """Add trace context to log message."""
        trace_context = get_current_trace_context()
        traceparent = trace_context.get("traceparent", "none")
        
        extra_msg = f" [trace={traceparent}]"
        if kwargs:
            extra_msg += f" {kwargs}"
        
        self.logger.log(level, msg + extra_msg)
    
    def debug(self, msg: str, **kwargs):
        self._log_with_trace(logging.DEBUG, msg, **kwargs)
    
    def info(self, msg: str, **kwargs):
        self._log_with_trace(logging.INFO, msg, **kwargs)
    
    def warning(self, msg: str, **kwargs):
        self._log_with_trace(logging.WARNING, msg, **kwargs)
    
    def error(self, msg: str, **kwargs):
        self._log_with_trace(logging.ERROR, msg, **kwargs)
    
    def critical(self, msg: str, **kwargs):
        self._log_with_trace(logging.CRITICAL, msg, **kwargs)


# Export commonly used items
__all__ = [
    "setup_telemetry",
    "get_tracer",
    "create_span",
    "traced",
    "traced_method",
    "extract_trace_context",
    "inject_trace_context",
    "get_current_trace_context",
    "add_span_attributes",
    "add_span_event",
    "set_span_error",
    "log_info",
    "log_error",
    "log_warning",
    "log_debug",
    "log_with_trace",
    "TraceLogger",
    "tracer",
]
