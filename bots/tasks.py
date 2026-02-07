"""
Celery tasks for cookie provider platform.
Each platform has its own queue for better load distribution.
Run workers with --concurrency=1 to ensure FIFO processing (one task at a time).
"""
import logging
import time
from celery import shared_task
from django.conf import settings
from bots.services.cookie_service import CookieService
from bots.services.logger import (
    traced,
    add_span_attributes, 
    add_span_event,
    inject_trace_context,
    log_info,
    log_error,
    log_warning,
    log_debug,
    TraceLogger
)

logger = logging.getLogger(__name__)
trace_logger = TraceLogger(__name__)

# Retry configuration
RETRY_DELAY = 10  # Wait 60 seconds before retrying when no cookies available


@shared_task(bind=True, queue='instagram_queue', name='bots.tasks.process_instagram_job')
def process_instagram_job(self, **kwargs):
    """
    Process Instagram cookie request and send to Lambda.
    
    Args:
        **kwargs: Job parameters including:
            - job_id: Job identifier from NestJS server
            - post_url: Post URL from NestJS server
            - callback_url: Callback URL from NestJS server
            - retry_count: Retry attempt number (optional, default: 0)
            - next_cursor: Cursor to resume from (optional)
    """
    return _process_platform_job('instagram', **kwargs)


@shared_task(bind=True, queue='linkedin_queue', name='bots.tasks.process_linkedin_job')
def process_linkedin_job(self, **kwargs):
    """
    Process LinkedIn cookie request and send to Lambda.
    
    Args:
        **kwargs: Job parameters (same as process_instagram_job)
    """
    return _process_platform_job('linkedin', **kwargs)


@shared_task(bind=True, queue='twitter_queue', name='bots.tasks.process_twitter_job')
def process_twitter_job(self, **kwargs):
    """
    Process Twitter cookie request and send to Lambda.
    
    Args:
        **kwargs: Job parameters (same as process_instagram_job)
    """
    return _process_platform_job('twitter', **kwargs)


def _process_platform_job(platform: str, **kwargs) -> dict:
    """
    Common logic for processing platform jobs.
    
    Simple FIFO queue: if no cookie available, sleep and retry.
    Since worker runs with --concurrency=1, this blocks the entire queue.
    
    Args:
        platform: Platform name (instagram, linkedin, twitter)
        **kwargs: Job parameters including:
            - job_id: Job identifier
            - post_url: Post URL
            - callback_url: Callback URL
            - retry_count: Retry attempt (optional, default: 0)
            - next_cursor: Resume cursor (optional)
        
    Returns:
        Dictionary with status and details
    """
    # Extract required parameters
    job_id = kwargs.get('job_id')
    post_url = kwargs.get('post_url')
    callback_url = kwargs.get('callback_url')
    retry_count = kwargs.get('retry_count', 0)
    next_cursor = kwargs.get('next_cursor')
    
    # Add tracing attributes for the job
    add_span_attributes(
        job_id=job_id,
        platform=platform,
        retry_count=retry_count,
        has_next_cursor=bool(next_cursor),
        is_retry=retry_count > 0
    )
    
    log_info(
        f"Processing {platform} job",
        job_id=job_id,
        platform=platform,
        retry_count=retry_count,
        has_cursor=bool(next_cursor)
    )
    
    cookie_data = None
    allocation_retry_count = 0
    
    while cookie_data is None:
        cookie_data = CookieService.allocate_cookie(platform, post_url=post_url)
        
        if cookie_data is None:
            allocation_retry_count += 1
            add_span_attributes(cookie_allocation_retries=allocation_retry_count)
            add_span_event("cookie_allocation_retry", {"retry_count": allocation_retry_count, "retry_delay": RETRY_DELAY})
            
            log_warning(
                f"No available cookies for {platform}. Blocking queue",
                job_id=job_id,
                platform=platform,
                allocation_retry=allocation_retry_count,
                retry_delay_seconds=RETRY_DELAY
            )
            time.sleep(RETRY_DELAY)
            log_info(
                "Retrying cookie allocation",
                job_id=job_id,
                platform=platform,
                allocation_retry=allocation_retry_count
            )
    
    add_span_attributes(cookie_id=cookie_data['cookie_id'], cookie_allocation_retries=allocation_retry_count)
    add_span_event("cookie_allocated", {"cookie_id": cookie_data['cookie_id'], "allocation_retries": allocation_retry_count})
    
    log_info(
        "Cookie allocated successfully",
        job_id=job_id,
        cookie_id=cookie_data['cookie_id'],
        comment_count=cookie_data.get('comment_count', 0),
        allocation_retries=allocation_retry_count
    )
    
    payload = {
        'post_url': post_url,
        'job_id': job_id,
        'cookie_id': cookie_data['cookie_id'],
        'callback_url': callback_url,
        'cookies_release_url': settings.COOKIE_RELEASE_URL,
        'trigger_job_url': settings.TRIGGER_JOB_URL,
        'cookies': cookie_data['cookies'],
        'csrf_token': cookie_data.get('csrf_token', ''),
        'expected_comment_count': cookie_data.get('comment_count', 0),  # From validation
        'retry_count': retry_count,  # Lambda retry count (not cookie allocation retry)
    }
    
    # Add optional next_cursor parameter
    if next_cursor:
        payload['next_cursor'] = next_cursor
        add_span_event("cursor_resumption", {
            "job_id": job_id,
            "retry_count": retry_count,
            "cursor_preview": next_cursor[:100] if len(next_cursor) > 100 else next_cursor
        })
        log_info(
            "Including next_cursor for job (resuming from cursor)",
            job_id=job_id,
            retry_count=retry_count
        )
    
    # Send to Lambda via fire-and-forget
    _fire_and_forget_lambda(
        settings.LAMBDA_FUNCTION_URL,
        payload,
        job_id,
        platform,
        cookie_data['cookie_id']
    )
    
    return {
        'status': 'success',
        'message': 'Cookie sent to Lambda',
        'job_id': job_id,
        'platform': platform,
        'cookie_id': cookie_data['cookie_id'],
        'retry_count': retry_count,
        'resuming_from_cursor': bool(next_cursor),
    }


@traced("lambda.invoke_fire_and_forget")
def _fire_and_forget_lambda(url: str, payload: dict, job_id: str, platform: str, cookie_id: int) -> None:
    """Fire-and-forget method to send payload to Lambda function."""
    import requests
    from requests.exceptions import ReadTimeout
    
    add_span_attributes(lambda_url=url, job_id=job_id, platform=platform, cookie_id=cookie_id)
    add_span_event("lambda_invocation_started", {"payload_size": len(str(payload))})
    
    try:
        log_info(
            "Invoking Lambda function",
            job_id=job_id,
            platform=platform,
            cookie_id=cookie_id,
            lambda_url=url
        )
        
        # Fire-and-forget: short timeout so we don't wait for Lambda processing
        response = requests.post(url, json=payload, timeout=(5.0, 3.0))
        
        if response.status_code >= 400:
            log_warning(
                "Lambda returned error status",
                job_id=job_id,
                platform=platform,
                cookie_id=cookie_id,
                status_code=response.status_code,
                lambda_url=url
            )
            add_span_attributes(http_status_code=response.status_code, lambda_success=False)
        else:
            log_info(
                "Lambda invocation successful",
                job_id=job_id,
                platform=platform,
                cookie_id=cookie_id,
                status_code=response.status_code
            )
            add_span_attributes(http_status_code=response.status_code, lambda_success=True)
        
        add_span_event("lambda_invocation_completed", {"status_code": response.status_code})
        log_info(
            "Lambda triggered (fire-and-forget)",
            job_id=job_id,
            platform=platform,
            cookie_id=cookie_id
        )
        
    except ReadTimeout:
        # ReadTimeout is expected in fire-and-forget (connection timeout is OK)
        add_span_event("lambda_timeout_expected", {"timeout_type": "read"})
        log_debug(
            "Lambda read timeout (expected for fire-and-forget)",
            job_id=job_id,
            platform=platform,
            cookie_id=cookie_id
        )
    except Exception as error:
        add_span_event("lambda_invocation_failed", {"error_type": type(error).__name__})
        log_error(
            "Failed to trigger Lambda",
            error=error,
            job_id=job_id,
            platform=platform,
            cookie_id=cookie_id,
            lambda_url=url
        )
