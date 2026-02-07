"""
Webhook endpoints for cookie provider platform.

This module provides two main endpoints:
1. /webhook/trigger-job/ - Triggers a Celery task to allocate and send a cookie
2. /webhook/release-cookie/ - Releases a cookie back to the available pool
"""
import requests
import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from bots.services.cookie_service import CookieService
from bots.services.logger import (
    traced,
    add_span_attributes,
    add_span_event,
    set_span_error,
    log_info,
    log_error,
    log_warning,
    TraceLogger
)
from bots.tasks import (
    process_instagram_job,
    process_linkedin_job,
    process_twitter_job,
)

logger = logging.getLogger(__name__)
trace_logger = TraceLogger(__name__)

# Map platform names to their respective Celery tasks
PLATFORM_TASKS = {
    'instagram': process_instagram_job,
    'linkedin': process_linkedin_job,
    'twitter': process_twitter_job,
}


@csrf_exempt
@require_http_methods(["POST"])
@traced("webhook.trigger_job")
def trigger_job(request):
    """
    Endpoint to trigger a Celery task for cookie allocation.
    
    Expected payload:
    {
        "job_id": "string",
        "platform": "instagram" | "linkedin" | "twitter",
        "post_url": "string",
        "callback_url": "string",
        "retry_count": number (optional, default: 0),
        "next_cursor": "string" (optional, for resuming scraping)
    }
    
    Response:
    {
        "status": "success" | "error",
        "message": "string",
        "task_id": "string" (only on success)
    }
    """
    try:
        data = json.loads(request.body)
        job_id = data.get('job_id')
        platform = data.get('platform', '').lower()
        post_url = data.get('post_url')
        callback_url = data.get('callback_url')
        
        # Optional retry parameters
        retry_count = data.get('retry_count', 0)
        next_cursor = data.get('next_cursor')
        
        # Add tracing attributes for all parameters
        add_span_attributes(
            job_id=job_id,
            platform=platform,
            has_post_url=bool(post_url),
            has_callback_url=bool(callback_url),
            retry_count=retry_count,
            has_next_cursor=bool(next_cursor),
            is_retry=retry_count > 0
        )
        
        # Validate required fields
        if not job_id:
            add_span_event("validation_failed", {"reason": "missing_job_id"})
            return JsonResponse({'status': 'error', 'message': 'job_id is required'}, status=400)
        
        if not platform:
            add_span_event("validation_failed", {"reason": "missing_platform"})
            return JsonResponse({'status': 'error', 'message': 'platform is required'}, status=400)
        
        if not post_url:
            add_span_event("validation_failed", {"reason": "missing_post_url"})
            return JsonResponse({'status': 'error', 'message': 'post_url is required'}, status=400)
        
        if not callback_url:
            add_span_event("validation_failed", {"reason": "missing_callback_url"})
            return JsonResponse({'status': 'error', 'message': 'callback_url is required'}, status=400)
        
        # Validate retry_count
        try:
            retry_count = int(retry_count)
            if retry_count < 0:
                retry_count = 0
        except (ValueError, TypeError):
            retry_count = 0
        
        # Get the appropriate task for the platform
        task = PLATFORM_TASKS.get(platform)
        if not task:
            add_span_event("validation_failed", {"reason": "invalid_platform"})
            return JsonResponse({
                'status': 'error',
                'message': f'Invalid platform: {platform}. Must be one of: {", ".join(PLATFORM_TASKS.keys())}'
            }, status=400)
        
        # Build task parameters
        task_params = {
            'job_id': job_id,
            'post_url': post_url,
            'callback_url': callback_url,
            'retry_count': retry_count,
        }
        
        # Add optional next_cursor parameter
        if next_cursor:
            task_params['next_cursor'] = next_cursor
            add_span_event("retry_with_cursor", {
                "job_id": job_id,
                "retry_count": retry_count,
                "cursor_preview": next_cursor[:100] if len(next_cursor) > 100 else next_cursor
            })
            logger.info(f"Retry triggered for job {job_id} with next_cursor (resuming from cursor)")
        
        # Trigger the Celery task with all parameters
        result = task.delay(**task_params)
        
        add_span_event("task_triggered", {
            "task_id": result.id,
            "platform": platform,
            "retry_count": retry_count,
            "has_cursor": bool(next_cursor)
        })
        
        log_info(
            f"Triggered {platform} task for job {job_id}",
            job_id=job_id,
            task_id=result.id,
            platform=platform,
            retry_count=retry_count,
            has_cursor=bool(next_cursor)
        )
        
        return JsonResponse({
            'status': 'success',
            'message': f'Task triggered for {platform} platform',
            'task_id': result.id,
            'job_id': job_id,
            'platform': platform,
            'retry_count': retry_count,
            'resuming_from_cursor': bool(next_cursor)
        }, status=202)
        
    except json.JSONDecodeError as e:
        set_span_error(e)
        return JsonResponse({'status': 'error', 'message': 'Invalid JSON payload'}, status=400)
    
    except Exception as e:
        set_span_error(e)
        log_error(
            "Error triggering job",
            error=e,
            job_id=job_id if 'job_id' in locals() else None,
            platform=platform if 'platform' in locals() else None
        )
        return JsonResponse({'status': 'error', 'message': f'Internal server error: {str(e)}'}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@traced("webhook.release_cookie")
def release_cookie(request):
    """Endpoint to release a cookie back to the available pool."""
    try:
        data = json.loads(request.body)
        cookie_id = data.get('cookie_id')
        cookie_success = data.get('cookie_success', True)
        failure_reason = data.get('failure_reason', None)
        
        add_span_attributes(
            cookie_id=cookie_id,
            cookie_success=cookie_success,
            failure_reason=failure_reason if failure_reason else "none"
        )
        
        if not cookie_id:
            add_span_event("validation_failed", {"reason": "missing_cookie_id"})
            return JsonResponse({'status': 'error', 'message': 'cookie_id is required'}, status=400)
        
        try:
            cookie_id = int(cookie_id)
        except (ValueError, TypeError):
            add_span_event("validation_failed", {"reason": "invalid_cookie_id_type"})
            return JsonResponse({'status': 'error', 'message': 'cookie_id must be a valid integer'}, status=400)
        
        success = CookieService.release_cookie(cookie_id, cookie_success, failure_reason)
        
        if success:
            status_msg = "successfully" if cookie_success else "with failure"
            add_span_event("cookie_released", {"cookie_id": cookie_id, "success": cookie_success})
            
            log_info(
                f"Cookie released {status_msg}",
                cookie_id=cookie_id,
                cookie_success=cookie_success,
                failure_reason=failure_reason if failure_reason else "none"
            )
            if not cookie_success and failure_reason:
                log_warning(
                    "Cookie marked as failed",
                    cookie_id=cookie_id,
                    failure_reason=failure_reason
                )
            
            return JsonResponse({
                'status': 'success',
                'message': f'Cookie released for cookie_id: {cookie_id}',
                'cookie_id': cookie_id,
                'cookie_success': cookie_success
            }, status=200)
        else:
            add_span_event("cookie_not_found", {"cookie_id": cookie_id})
            return JsonResponse({'status': 'error', 'message': f'Cookie not found for cookie_id: {cookie_id}'}, status=404)
        
    except json.JSONDecodeError as e:
        set_span_error(e)
        return JsonResponse({'status': 'error', 'message': 'Invalid JSON payload'}, status=400)
    
    except Exception as e:
        set_span_error(e)
        log_error(
            "Error releasing cookie",
            error=e,
            cookie_id=cookie_id if 'cookie_id' in locals() else None
        )
        return JsonResponse({'status': 'error', 'message': f'Internal server error: {str(e)}'}, status=500)


@require_http_methods(["GET"])
def health_check(request):
    """
    Simple health check endpoint.
    """
    return JsonResponse({
        'status': 'healthy',
        'service': 'cookie-provider-webhook'
    })

