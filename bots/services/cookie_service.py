"""
Cookie Service: Handles cookie allocation and release logic.
Implements LRU (Least Recently Used) selection strategy with pre-allocation validation.
"""
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from typing import Optional, Dict, Any
from bots.models import SocialAccount
from bots.services.logger import (
    traced_method,
    add_span_attributes,
    add_span_event,
    log_info,
    log_warning,
    log_error,
    log_debug
)
from bots.services.cookie_validator import CookieValidator


class CookieService:
    """
    Service for managing cookie allocation and release.
    
    This service ensures:
    - Only logged-in accounts are provided
    - LRU (Least Recently Used) strategy for cookie selection
    - Thread-safe cookie allocation using database transactions
    - Proper tracking of cookie usage status
    """
    
    PLATFORM_MAP = {
        'instagram': 'IG',
        'linkedin': 'LI',
        'twitter': 'TW',
    }
    
    @staticmethod
    def convert_cookies_to_string(cookies_list: list) -> tuple[str, str]:
        """
        Convert cookies array to string format and extract csrf_token.
        
        Args:
            cookies_list: List of cookie objects with 'name' and 'value' keys
            
        Returns:
            Tuple of (cookies_string, csrf_token)
        """
        cookie_parts = []
        csrf_token = ''
        
        if cookies_list and isinstance(cookies_list, list):
            for cookie in cookies_list:
                if isinstance(cookie, dict):
                    name = cookie.get('name', '')
                    value = cookie.get('value', '')
                    if name and value:
                        # Extract csrf_token
                        if name == 'csrftoken':
                            csrf_token = value.strip('"')
                        # Remove surrounding quotes from value if present
                        value = value.strip('"')
                        cookie_parts.append(f"{name}={value}")
        
        cookies_string = "; ".join(cookie_parts)
        return cookies_string, csrf_token
    
    @classmethod
    def get_platform_code(cls, platform_name: str) -> Optional[str]:
        """
        Convert platform name to platform code.
        
        Args:
            platform_name: Name of the platform (instagram, linkedin, twitter)
            
        Returns:
            Platform code (IG, LI, TW) or None if invalid
        """
        return cls.PLATFORM_MAP.get(platform_name.lower())
    
    @classmethod
    @traced_method("cookie_service.allocate_cookie")
    @transaction.atomic
    def allocate_cookie(cls, platform: str, post_url: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Allocate a cookie for the given platform using LRU strategy.
        
        This method:
        1. Finds available cookies (logged_in=True, in_use=False)
        2. Selects the least recently used one
        3. Marks it as in_use
        4. Validates cookie using post_url
        5. Updates last_used_at timestamp
        6. Returns cookie data
        
        Args:
            platform: Platform name (instagram, linkedin, twitter)
            post_url: Optional post URL to validate against (recommended for Instagram)
            
        Returns:
            Dictionary with cookie data and metadata, or None if no cookie available
        """
        platform_code = cls.get_platform_code(platform)
        if not platform_code:
            log_error(
                "Invalid platform requested",
                platform=platform,
                valid_platforms=list(cls.PLATFORM_MAP.keys())
            )
            return None
        
        add_span_attributes(platform=platform, platform_code=platform_code)
        add_span_event("cookie_allocation_started", {"platform": platform})
        
        # Select available accounts using select_for_update to prevent race conditions
        # Order by last_used_at (NULL first for never-used cookies, then oldest first)
        account = (
            SocialAccount.objects
            .select_for_update()
            .filter(
                platform=platform_code,
                logged_in=True,
                in_use=False,
            )
            .order_by('last_used_at')  # NULL values come first, then oldest
            .first()
        )
        
        if not account:
            log_warning(
                "No available cookies for platform",
                platform=platform,
                platform_code=platform_code
            )
            add_span_event("cookie_allocation_failed", {"reason": "no_available_cookies"})
            return None
        
        # Mark as in use
        account.in_use = True
        account.save(update_fields=['in_use'])
        
        # Convert cookies array to string format and extract csrf_token
        cookies_string, csrf_token = cls.convert_cookies_to_string(account.cookies)
        
        add_span_attributes(
            cookie_id=account.id,
            username=account.username,
            cookie_count=len(account.cookies) if account.cookies else 0,
            has_csrf_token=bool(csrf_token),
            last_used_at=account.last_used_at.isoformat() if account.last_used_at else "never",
            last_validated_at=account.last_validated_at.isoformat() if account.last_validated_at else "never"
        )
        
        # 🆕 VALIDATE COOKIE SESSION BEFORE RETURNING
        # This prevents wasting Lambda execution on invalid/expired/rate-limited cookies
        add_span_event("cookie_validation_starting", {
            "cookie_id": account.id,
            "platform": platform,
            "username": account.username
        })
        
        is_valid, failure_reason, comment_count = CookieValidator.validate(
            cookies=cookies_string,
            csrf_token=csrf_token,
            platform=platform,
            cookie_id=account.id,  # For sticky proxy session
            post_url=post_url  # For Instagram post-specific validation
        )
        
        if not is_valid:
            # Cookie validation failed - mark as logged out immediately
            account.logged_in = False
            account.in_use = False
            account.increment_failures(reason=f"Pre-validation failed: {failure_reason}")
            account.save(update_fields=['logged_in', 'in_use'])
            
            add_span_attributes(
                validation_failed=True,
                failure_reason=failure_reason,
                consecutive_failures=account.consecutive_failures
            )
            add_span_event("cookie_validation_failed_marked_logged_out", {
                "cookie_id": account.id,
                "username": account.username,
                "failure_reason": failure_reason,
                "consecutive_failures": account.consecutive_failures,
                "was_banned": account.consecutive_failures >= 3
            })
            
            log_warning(
                "Cookie validation failed, marked as logged out",
                cookie_id=account.id,
                username=account.username,
                platform=platform,
                failure_reason=failure_reason,
                consecutive_failures=account.consecutive_failures,
                banned=account.consecutive_failures >= 3
            )
            
            # Return None to trigger retry loop in tasks.py
            return None
        
        # Cookie is valid! Update validation timestamp
        account.last_validated_at = timezone.now()
        account.save(update_fields=['last_validated_at'])
        
        add_span_attributes(validation_success=True, comment_count=comment_count)
        add_span_event("cookie_validated_successfully", {
            "cookie_id": account.id,
            "username": account.username,
            "comment_count": comment_count
        })
        
        add_span_event("cookie_allocated", {
            "cookie_id": account.id,
            "username": account.username,
            "was_used_before": account.last_used_at is not None,
            "comment_count": comment_count
        })
        
        log_info(
            "Cookie allocated successfully",
            cookie_id=account.id,
            username=account.username,
            platform=platform,
            comment_count=comment_count,
            last_used_at=account.last_used_at.isoformat() if account.last_used_at else "never"
        )
        
        # Return cookie data in the format needed by Lambda
        return {
            'cookie_id': account.id,
            'username': account.username,
            'platform': platform,
            'cookies': cookies_string,
            'csrf_token': csrf_token,
            'comment_count': comment_count,  # 🆕 Extracted during validation (Instagram only)
        }
    
    @classmethod
    @traced_method("cookie_service.release_cookie")
    @transaction.atomic
    def release_cookie(cls, cookie_id: int, success: bool = True, failure_reason: str = None) -> bool:
        """
        Release a cookie by setting in_use to False and tracking success/failure.
        
        Args:
            cookie_id: ID of the SocialAccount
            success: Whether the cookie was used successfully
            failure_reason: Reason for failure (if success=False)
            
        Returns:
            True if cookie was released, False if not found
        """
        add_span_attributes(
            cookie_id=cookie_id,
            success=success,
            has_failure_reason=failure_reason is not None
        )
        add_span_event("cookie_release_started", {
            "cookie_id": cookie_id,
            "success": success
        })
        
        try:
            account = SocialAccount.objects.select_for_update().get(id=cookie_id)
            account.in_use = False
            account.last_used_at = timezone.now()
            
            if success:
                # Reset failure counter on successful use
                previous_failures = account.consecutive_failures
                account.reset_failures()
                add_span_attributes(
                    consecutive_failures_reset_from=previous_failures,
                    username=account.username
                )
                log_info(
                    "Cookie released successfully",
                    cookie_id=cookie_id,
                    username=account.username,
                    platform=account.get_platform_display(),
                    previous_failures=previous_failures
                )
            else:
                # Increment failure counter
                account.increment_failures(reason=failure_reason)
                add_span_attributes(
                    consecutive_failures=account.consecutive_failures,
                    failure_reason=failure_reason or "unknown",
                    username=account.username
                )
                add_span_event("cookie_failure_recorded", {
                    "cookie_id": cookie_id,
                    "consecutive_failures": account.consecutive_failures,
                    "reason": failure_reason or "unknown"
                })
                log_warning(
                    "Cookie released with failure",
                    cookie_id=cookie_id,
                    username=account.username,
                    platform=account.get_platform_display(),
                    consecutive_failures=account.consecutive_failures,
                    failure_reason=failure_reason or "unknown"
                )
            
            account.save(update_fields=['in_use', 'last_used_at'])
            add_span_event("cookie_released", {"cookie_id": cookie_id})
            return True
        except SocialAccount.DoesNotExist:
            log_error(
                "Cookie not found for release",
                cookie_id=cookie_id
            )
            add_span_event("cookie_release_failed", {"reason": "not_found"})
            return False
    
    @classmethod
    @traced_method("cookie_service.get_cookie_info")
    def get_cookie_info(cls, cookie_id: int) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific cookie.
        
        Args:
            cookie_id: ID of the SocialAccount
            
        Returns:
            Dictionary with account info or None if not found
        """
        try:
            account = SocialAccount.objects.get(id=cookie_id)
            return {
                'cookie_id': account.id,
                'username': account.username,
                'platform': account.get_platform_display(),
                'logged_in': account.logged_in,
                'in_use': account.in_use,
                'last_used_at': account.last_used_at.isoformat() if account.last_used_at else None,
            }
        except SocialAccount.DoesNotExist:
            return None
