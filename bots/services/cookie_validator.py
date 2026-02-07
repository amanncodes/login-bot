"""
Cookie Validator: Validates cookie sessions before allocation.
Uses sticky proxy to ensure validation happens on same IP as scraping.
"""
import os
import requests
from typing import Tuple
from bots.services.logger import (
    traced_method,
    add_span_attributes,
    add_span_event,
    log_info,
    log_warning,
    log_error,
)


class CookieValidator:
    """
    Validates cookie sessions using lightweight API calls.
    
    Validation Strategy:
    - Instagram: GraphQL media info endpoint (validates access to specific post + extracts comment_count)
      - Retries with exponential backoff (2s, 4s, 8s)
      - Requires post_url parameter
    - LinkedIn: GET /voyager/api/me
    - Twitter: GET /i/api/1.1/account/verify_credentials.json
    
    Sticky Proxy:
    - Uses cookie_id to create consistent proxy session
    - Format: {proxy_username}-cookie-{cookie_id}@{proxy_host}:{proxy_port}
    - Same IP for validation AND scraping (Lambda uses same pattern)
    """
    
    # Validation endpoints
    INSTAGRAM_GRAPHQL_URL = "https://www.instagram.com/graphql/query"
    INSTAGRAM_MEDIA_INFO_DOC_ID = "25018359077785073"  # Same as Lambda scraper
    LINKEDIN_VALIDATION_URL = "https://www.linkedin.com/voyager/api/me"
    TWITTER_VALIDATION_URL = "https://api.twitter.com/1.1/account/verify_credentials.json"
    
    # Timeouts
    VALIDATION_TIMEOUT = 5  # seconds
    
    # Proxy config from environment
    PROXY_HOST = os.getenv('PROXY_HOST')
    PROXY_PORT = os.getenv('PROXY_PORT')
    PROXY_USERNAME = os.getenv('PROXY_USERNAME')
    PROXY_PASSWORD = os.getenv('PROXY_PASSWORD')
    
    @classmethod
    def _get_proxies(cls, cookie_id: int):
        """
        Create proxy configuration with sticky session based on cookie_id.
        
        This ensures validation uses the SAME IP that Lambda will use for scraping.
        Format: username-cookie-{cookie_id}@proxy (e.g., user-cookie-123@proxy.com)
        
        Args:
            cookie_id: Cookie ID for sticky session
            
        Returns:
            Proxies dict for requests library or None if proxy not configured
        """
        if not all([cls.PROXY_HOST, cls.PROXY_PORT, cls.PROXY_USERNAME, cls.PROXY_PASSWORD]):
            return None
        
        # Create sticky session username using cookie_id
        # This matches Lambda's pattern: username-session-{uuid} → username-cookie-{cookie_id}
        sticky_username = f"{cls.PROXY_USERNAME}-cookie-{cookie_id}"
        proxy_url = f"http://{sticky_username}:{cls.PROXY_PASSWORD}@{cls.PROXY_HOST}:{cls.PROXY_PORT}"
        
        return {
            'http': proxy_url,
            'https': proxy_url
        }
    
    @classmethod
    def _extract_shortcode_from_url(cls, post_url: str) -> str:
        """Extract shortcode from Instagram post URL."""
        import re
        shortcode_match = re.search(r'/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)', post_url)
        return shortcode_match.group(1) if shortcode_match else None
    
    @classmethod
    def _validate_with_post_graphql(cls, post_url: str, headers: dict, proxies: dict, cookie_id: int) -> Tuple[bool, str, int]:
        """
        Validate cookie by accessing Instagram GraphQL media info endpoint.
        Also extracts comment_count for Lambda to use.
        
        Implements retry logic with exponential backoff: 2s, 4s, 8s
        
        Returns:
            (is_valid, failure_reason, comment_count)
        """
        import json as json_module
        import time
        
        shortcode = cls._extract_shortcode_from_url(post_url)
        if not shortcode:
            return (False, "invalid_shortcode", 0)
        
        add_span_event("validating_with_post", {"shortcode": shortcode})
        log_info(
            "Validating cookie with post URL (GraphQL media info)",
            cookie_id=cookie_id,
            shortcode=shortcode
        )
        
        # Use GraphQL media info endpoint (same as Lambda scraper)
        variables = {"shortcode": shortcode}
        params = {
            'variables': json_module.dumps(variables),
            'doc_id': cls.INSTAGRAM_MEDIA_INFO_DOC_ID
        }
        
        # Retry with exponential backoff: 2s, 4s, 8s
        retry_delays = [2, 4, 8]
        last_error = None
        
        for attempt in range(len(retry_delays) + 1):
            try:
                if attempt > 0:
                    delay = retry_delays[attempt - 1]
                    add_span_event("validation_retry", {
                        "attempt": attempt + 1,
                        "delay_seconds": delay,
                        "shortcode": shortcode
                    })
                    log_info(
                        f"Retrying validation after {delay}s",
                        cookie_id=cookie_id,
                        attempt=attempt + 1,
                        delay_seconds=delay
                    )
                    time.sleep(delay)
                
                response = requests.get(
                    cls.INSTAGRAM_GRAPHQL_URL,
                    params=params,
                    headers=headers,
                    proxies=proxies,
                    timeout=cls.VALIDATION_TIMEOUT
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Check if we can access the post and extract comment_count
                items = data.get('data', {}).get('xdt_api__v1__media__shortcode__web_info', {}).get('items', [])
                if items and len(items) > 0:
                    media_item = items[0]
                    comment_count = media_item.get('comment_count', 0)
                    
                    add_span_attributes(
                        validation_success=True,
                        http_status=response.status_code,
                        validation_method="post_graphql",
                        comment_count=comment_count,
                        retry_attempts=attempt
                    )
                    add_span_event("validation_success", {
                        "cookie_id": cookie_id,
                        "platform": "instagram",
                        "method": "post_graphql",
                        "comment_count": comment_count,
                        "retry_attempts": attempt
                    })
                    log_info(
                        "Cookie validation successful (GraphQL post)",
                        cookie_id=cookie_id,
                        platform="instagram",
                        shortcode=shortcode,
                        comment_count=comment_count,
                        retry_attempts=attempt
                    )
                    return (True, "", comment_count)
                else:
                    # Unexpected response structure - retry
                    log_warning(
                        "Instagram GraphQL validation: unexpected response structure, will retry",
                        cookie_id=cookie_id,
                        shortcode=shortcode,
                        attempt=attempt + 1
                    )
                    last_error = "unexpected_response"
                    if attempt == len(retry_delays):
                        # Final attempt failed
                        return (False, "unexpected_response", 0)
                    continue
            
            except requests.exceptions.HTTPError as e:
                # HTTP errors shouldn't retry - return immediately
                raise
            except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                # Network/timeout errors - retry
                last_error = e
                log_warning(
                    f"Validation attempt {attempt + 1} failed with network error",
                    cookie_id=cookie_id,
                    error=str(e),
                    attempt=attempt + 1
                )
                if attempt == len(retry_delays):
                    # Final attempt failed - re-raise to be caught by outer handler
                    raise
                continue
        
        # Should not reach here, but just in case
        return (False, "max_retries_exceeded", 0)
    
    @classmethod
    @traced_method("cookie_validator.validate_instagram")
    def validate_instagram(cls, cookies: str, csrf_token: str, cookie_id: int, post_url: str) -> Tuple[bool, str, int]:
        """
        Validate Instagram cookie session using GraphQL media info endpoint.
        
        Validation Strategy:
        - Validates by accessing the specific post's media info via GraphQL
        - Natural (different post each time)
        - Validates exact permission needed
        - Extracts comment_count for Lambda (eliminates extra API call)
        - Retries with exponential backoff (2s, 4s, 8s) on transient failures
        
        Uses sticky proxy based on cookie_id.
        
        Args:
            cookies: Cookie string
            csrf_token: CSRF token
            cookie_id: Cookie ID for sticky proxy session
            post_url: Post URL to validate against (REQUIRED)
            
        Returns:
            (is_valid, failure_reason, comment_count)
            - (True, "", comment_count) if valid
            - (False, "session_expired", 0) if 401/403
            - (False, "rate_limited", 0) if 429
            - (False, "api_error", 0) if 5xx
            - (False, "network_error", 0) if timeout/connection failed
        """
        add_span_attributes(
            platform="instagram",
            cookie_id=cookie_id,
            proxy_enabled=bool(cls.PROXY_HOST)
        )
        add_span_event("validation_started", {
            "platform": "instagram",
            "cookie_id": cookie_id
        })
        
        headers = {
            'Cookie': cookies,
            'X-CSRFToken': csrf_token,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'x-ig-app-id': '936619743392459',
            'x-requested-with': 'XMLHttpRequest',
        }
        
        # Get sticky proxy configuration
        proxies = cls._get_proxies(cookie_id)
        if proxies:
            add_span_attributes(proxy_session=f"cookie-{cookie_id}")
            log_info(
                "Using sticky proxy for validation",
                cookie_id=cookie_id,
                proxy_session=f"cookie-{cookie_id}"
            )
        
        try:
            # Validate using GraphQL endpoint with retry logic
            return cls._validate_with_post_graphql(post_url, headers, proxies, cookie_id)
        
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            add_span_attributes(validation_success=False, http_status=status_code)
            
            if status_code in [401, 403]:
                # Session expired or unauthorized
                add_span_event("validation_failed", {"reason": "session_expired", "http_status": status_code})
                log_warning(
                    "Cookie validation failed: session expired",
                    cookie_id=cookie_id,
                    http_status=status_code,
                    platform="instagram"
                )
                return (False, "session_expired", 0)
            
            elif status_code == 429:
                # Rate limited
                add_span_event("validation_failed", {"reason": "rate_limited", "http_status": 429})
                log_warning(
                    "Cookie validation failed: rate limited",
                    cookie_id=cookie_id,
                    platform="instagram"
                )
                return (False, "rate_limited", 0)
            
            elif status_code >= 500:
                # Server error - treat as cookie failure (no benefit of doubt)
                add_span_event("validation_failed", {"reason": "api_error", "http_status": status_code})
                log_warning(
                    "Cookie validation failed: Instagram API error",
                    cookie_id=cookie_id,
                    http_status=status_code,
                    platform="instagram"
                )
                return (False, "api_error", 0)
            
            else:
                # Other HTTP errors
                add_span_event("validation_failed", {"reason": "http_error", "http_status": status_code})
                log_warning(
                    "Cookie validation failed: HTTP error",
                    cookie_id=cookie_id,
                    http_status=status_code,
                    platform="instagram"
                )
                return (False, f"http_error_{status_code}", 0)
        
        except requests.exceptions.Timeout as e:
            # Request timeout
            add_span_attributes(validation_success=False, failure_reason="timeout")
            add_span_event("validation_failed", {"reason": "timeout"})
            log_error(
                "Cookie validation failed: request timeout",
                error=e,
                cookie_id=cookie_id,
                platform="instagram"
            )
            return (False, "timeout", 0)
        
        except requests.exceptions.RequestException as e:
            # Network error (connection refused, DNS failure, etc.)
            add_span_attributes(validation_success=False, failure_reason="network_error")
            add_span_event("validation_failed", {"reason": "network_error"})
            log_error(
                "Cookie validation failed: network error",
                error=e,
                cookie_id=cookie_id,
                platform="instagram"
            )
            return (False, "network_error", 0)
        
        except requests.exceptions.JSONDecodeError as e:
            # Invalid JSON response
            add_span_attributes(validation_success=False, failure_reason="invalid_json")
            add_span_event("validation_failed", {"reason": "invalid_json"})
            log_error(
                "Cookie validation failed: invalid JSON response",
                error=e,
                cookie_id=cookie_id,
                platform="instagram"
            )
            return (False, "invalid_json", 0)
        
        except Exception as e:
            # Unexpected error
            add_span_attributes(validation_success=False, failure_reason="unexpected_error")
            add_span_event("validation_failed", {"reason": "unexpected_error", "error_type": type(e).__name__})
            log_error(
                "Cookie validation failed: unexpected error",
                error=e,
                cookie_id=cookie_id,
                platform="instagram",
                error_type=type(e).__name__
            )
            return (False, "unexpected_error", 0)
    
    @classmethod
    @traced_method("cookie_validator.validate_linkedin")
    def validate_linkedin(cls, cookies: str, csrf_token: str, cookie_id: int) -> Tuple[bool, str, int]:
        """
        Validate LinkedIn cookie session.
        
        Args:
            cookies: Cookie string
            csrf_token: CSRF token
            cookie_id: Cookie ID for sticky proxy session
            
        Returns:
            (is_valid, failure_reason, comment_count)
        """
        add_span_attributes(platform="linkedin", cookie_id=cookie_id)
        add_span_event("validation_started", {"platform": "linkedin", "cookie_id": cookie_id})
        
        # TODO: Implement LinkedIn validation
        # For now, return True (skip validation)
        log_info("LinkedIn validation not yet implemented, skipping", cookie_id=cookie_id)
        return (True, "", 0)
    
    @classmethod
    @traced_method("cookie_validator.validate_twitter")
    def validate_twitter(cls, cookies: str, csrf_token: str, cookie_id: int) -> Tuple[bool, str, int]:
        """
        Validate Twitter cookie session.
        
        Args:
            cookies: Cookie string
            csrf_token: CSRF token
            cookie_id: Cookie ID for sticky proxy session
            
        Returns:
            (is_valid, failure_reason, comment_count)
        """
        add_span_attributes(platform="twitter", cookie_id=cookie_id)
        add_span_event("validation_started", {"platform": "twitter", "cookie_id": cookie_id})
        
        # TODO: Implement Twitter validation
        # For now, return True (skip validation)
        log_info("Twitter validation not yet implemented, skipping", cookie_id=cookie_id)
        return (True, "", 0)
    
    @classmethod
    @traced_method("cookie_validator.validate")
    def validate(cls, cookies: str, csrf_token: str, platform: str, cookie_id: int, post_url: str = None) -> Tuple[bool, str, int]:
        """
        Validate cookie for the given platform.
        
        Args:
            cookies: Cookie string
            csrf_token: CSRF token
            platform: Platform name (instagram, linkedin, twitter)
            cookie_id: Cookie ID for sticky proxy session
            post_url: Optional post URL for Instagram validation (validates access to specific post)
            
        Returns:
            (is_valid, failure_reason, comment_count)
            - comment_count is extracted from Instagram GraphQL if post_url provided, else 0
        """
        add_span_attributes(
            platform=platform,
            cookie_id=cookie_id,
            has_cookies=bool(cookies),
            has_csrf_token=bool(csrf_token)
        )
        
        if platform.lower() == 'instagram':
            return cls.validate_instagram(cookies, csrf_token, cookie_id, post_url=post_url)
        elif platform.lower() == 'linkedin':
            return cls.validate_linkedin(cookies, csrf_token, cookie_id)
        elif platform.lower() == 'twitter':
            return cls.validate_twitter(cookies, csrf_token, cookie_id)
        else:
            log_warning("Unknown platform for validation, skipping", platform=platform, cookie_id=cookie_id)
            return (True, "", 0)  # Skip validation for unknown platforms
