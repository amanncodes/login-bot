"""
AWS Lambda Function for Instagram Comment Scraping

This is a standalone script that can be deployed as an AWS Lambda function.
Uses Instagram GraphQL API for comment scraping.

Environment Variables (Optional - can also be passed in request):
    - INSTAGRAM_COOKIES: Instagram session cookies
    - INSTAGRAM_CSRF_TOKEN: CSRF token for authentication

Deployment:
    1. Create a Lambda function in AWS
    2. Upload as a zip with dependencies (requests)
    3. Set handler to: lambda_function.lambda_handler
    4. Increase timeout to 5 minutes (300 seconds)
    5. Memory: 512 MB recommended

Test Event Example:
    {
        "job_id": "unique-job-id",
        "post_url": "https://www.instagram.com/reel/ABC123/",
        "callback_url": "https://your-api.com/webhook",
        "cookies": "your-cookies-here",
        "csrf_token": "your-csrf-token",
        "max_comments": 500
    }
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib import request, error, parse

# OpenTelemetry imports
from opentelemetry import trace
from lambda_telemetry import (
    setup_lambda_telemetry,
    traced,
    traced_method,
    traced_lambda_handler,
    add_span_attributes,
    add_span_event,
    set_span_error,
    traced_http_request,
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize OpenTelemetry (call once at module level)
setup_lambda_telemetry(
    service_name="instagram-comments-lambda",
    service_version="1.0.0",
)

# Constants for retry logic
MAX_RETRY_COUNT = 5
COMMENT_THRESHOLD_PERCENT = 0.80  # 80% threshold

# Lambda function URL for self-invocation retries
LAMBDA_FUNCTION_URL = "https://4a4fadvw2ovchgzhqk6w6bdaje0qmiin.lambda-url.ap-south-1.on.aws/"

# Default Hiker API Key (fallback when retry count exhausted)
DEFAULT_HIKER_API_KEY = "lyjwxdpzatek6whrirzrh3cjohim5811"


class InstagramAPIBlockedException(Exception):
    """
    Exception raised when Instagram API returns empty/invalid JSON responses
    consistently, indicating the API is blocked or rate-limited.
    This triggers immediate fallback to Hiker API.
    """
    pass


@traced("tracker_api.fetch_expected_comment_count")
def fetch_expected_comment_count(post_url: str) -> Optional[int]:
    """
    Fetch the expected total comment count from the tracker API.
    
    Args:
        post_url: Instagram post URL
        
    Returns:
        Expected comment count or None if fetch fails
    """
    try:
        encoded_url = parse.quote(post_url, safe='')
        api_url = f"https://tracker-dev-api.meldit.ai/api/v1/bot/get-post-by-post-url/placeholder?post_url={encoded_url}"
        
        req = request.Request(
            api_url,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Instagram-Lambda-Scraper/1.0'
            },
            method='GET'
        )
        
        with request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if data.get('success') and data.get('data'):
                post_counts = data['data'].get('post_counts', {})
                expected_comments = post_counts.get('comments', 0)
                logger.info(f"Expected comment count from API: {expected_comments}")
                return expected_comments
            
            logger.warning(f"API response success=False or no data: {data}")
            return None
            
    except error.HTTPError as e:
        logger.error(f"HTTP Error fetching expected comments: {e.code}")
        return None
    except error.URLError as e:
        logger.error(f"URL Error fetching expected comments: {e}")
        return None
    except Exception as e:
        logger.error(f"Error fetching expected comments: {e}")
        return None


def _fire_retry_request(lambda_url: str, payload: bytes) -> bool:
    """
    Fire the retry request to invoke Lambda.
    Uses a very short timeout to initiate the request without waiting for full response.
    
    Returns:
        True if request was initiated successfully, False otherwise
    """
    try:
        req = request.Request(
            lambda_url,
            data=payload,
            method='POST',
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Instagram-Lambda-Scraper/1.0'
            }
        )
        
        # Use a short timeout - we just need to initiate the request
        # The Lambda will continue running even if we don't wait for full response
        with request.urlopen(req, timeout=10) as response:
            # Read a small portion to confirm request was received
            logger.info(f"Lambda retry invocation response: {response.status}")
            return True
            
    except Exception as e:
        logger.error(f"Error invoking Lambda retry: {e}")
        return False


@traced("lambda.invoke_retry")
def invoke_lambda_retry(event: Dict[str, Any], retry_count: int) -> bool:
    """
    Invoke the same Lambda function with incremented retry count via function URL.
    Makes a synchronous HTTP request to trigger the retry Lambda.
    
    Args:
        event: Original event data
        retry_count: Current retry count
        
    Returns:
        True if invocation was triggered, False otherwise
    """
    try:
        # Create new event with incremented retry count
        retry_event = event.copy()
        retry_event['retry_count'] = retry_count + 1
        
        logger.info(f"Invoking Lambda retry via function URL with retry_count={retry_count + 1}")
        
        # Use Lambda function URL for self-invocation
        lambda_url = os.environ.get('LAMBDA_FUNCTION_URL', LAMBDA_FUNCTION_URL)
        
        payload = json.dumps(retry_event, ensure_ascii=False, default=str).encode('utf-8')
        
        # Make synchronous request to invoke the retry Lambda
        result = _fire_retry_request(lambda_url, payload)
        
        if result:
            logger.info(f"Lambda retry invocation successful")
        else:
            logger.error(f"Lambda retry invocation failed")
        
        return result
        
    except Exception as e:
        logger.error(f"Error invoking Lambda retry: {e}")
        return False


def should_retry(fetched_count: int, expected_count: int, retry_count: int) -> bool:
    """
    Determine if a retry is needed based on comment count threshold.
    
    Args:
        fetched_count: Number of comments actually fetched
        expected_count: Expected number of comments from API
        retry_count: Current retry count
        
    Returns:
        True if retry is needed, False otherwise
    """
    if retry_count >= MAX_RETRY_COUNT:
        logger.info(f"Max retry count ({MAX_RETRY_COUNT}) reached, no more retries")
        return False
    
    if expected_count <= 0:
        logger.info("Expected count is 0 or negative, no retry needed")
        return False
    
    threshold = expected_count * COMMENT_THRESHOLD_PERCENT
    
    if fetched_count >= threshold:
        logger.info(f"Fetched {fetched_count}/{expected_count} comments ({fetched_count/expected_count*100:.1f}%), threshold met")
        return False
    
    logger.warning(f"Fetched {fetched_count}/{expected_count} comments ({fetched_count/expected_count*100:.1f}%), below {COMMENT_THRESHOLD_PERCENT*100}% threshold, retry needed")
    return True


class InstagramCommentScraper:
    """
    Instagram comment scraper using GraphQL API.
    Designed for AWS Lambda.
    """
    
    def __init__(self, cookies: str, csrf_token: str):
        """
        Initialize the scraper with authentication.
        
        Args:
            cookies: Instagram session cookies
            csrf_token: CSRF token
        """
        self.cookies = cookies
        self.csrf_token = csrf_token
        self.url = "https://www.instagram.com/graphql/query"
        self.comments_doc_id = "25060748103519434"
        self.media_info_doc_id = "25018359077785073"
        self.replies_doc_id = "25042984138668372"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': self.cookies,
            'X-CSRFToken': self.csrf_token,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'x-ig-app-id': '936619743392459',
            'x-requested-with': 'XMLHttpRequest'
        }
    
    def extract_shortcode(self, url_or_shortcode: str) -> Optional[str]:
        """
        Extract shortcode from Instagram URL.
        
        Supported formats:
            - https://www.instagram.com/p/SHORTCODE/
            - https://www.instagram.com/reel/SHORTCODE/
            - https://www.instagram.com/reels/SHORTCODE/
            - SHORTCODE (directly)
        
        Args:
            url_or_shortcode: Instagram URL or shortcode
            
        Returns:
            Shortcode or None
        """
        # Pattern for URL (supports /p/, /reel/, and /reels/)
        pattern = r'instagram\.com/(?:p|reels?)/([A-Za-z0-9_-]+)'
        match = re.search(pattern, url_or_shortcode)
        if match:
            return match.group(1)
        
        # Check if it's already a shortcode (alphanumeric, 10-12 chars typically)
        if re.match(r'^[A-Za-z0-9_-]{6,}$', url_or_shortcode):
            return url_or_shortcode
        
        return None
    
    def _make_traced_instagram_request(self, encoded_data: bytes, headers: Dict[str, str], doc_id: str) -> Dict:
        """Make a traced HTTP POST request to Instagram API."""
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            f"POST {self.url}",
            kind=trace.SpanKind.CLIENT
        ) as http_span:
            http_span.set_attribute("http.method", "POST")
            http_span.set_attribute("http.url", self.url)
            http_span.set_attribute("http.scheme", "https")
            http_span.set_attribute("http.host", "www.instagram.com")
            http_span.set_attribute("instagram.doc_id", doc_id)
            
            try:
                req = request.Request(self.url, data=encoded_data, headers=headers, method='POST')
                with request.urlopen(req, timeout=60) as response:
                    http_span.set_attribute("http.status_code", response.status)
                    response_text = response.read().decode('utf-8')
                    result = json.loads(response_text)
                    http_span.set_status(trace.Status(trace.StatusCode.OK))
                    return result
            except Exception as req_error:
                http_span.record_exception(req_error)
                http_span.set_status(trace.Status(trace.StatusCode.ERROR, str(req_error)))
                raise
    
    @traced_method("instagram.api_request")
    def _make_request(self, data: Dict[str, str], max_retries: int = 3) -> Optional[Dict]:
        """
        Make HTTP POST request to Instagram API.
        
        Args:
            data: Form data for the request
            max_retries: Maximum retry attempts
            
        Returns:
            JSON response or None
            
        Raises:
            InstagramAPIBlockedException: When JSON decode errors occur consistently
        """
        encoded_data = parse.urlencode(data).encode('utf-8')
        headers = self._get_headers()
        json_decode_errors = 0  # Track consecutive JSON decode errors
        
        # Add span attributes
        add_span_attributes(
            api_url=self.url,
            doc_id=data.get('doc_id', 'unknown'),
            max_retries=max_retries
        )
        
        for attempt in range(max_retries):
            add_span_event(f"attempt_{attempt + 1}", {"attempt": attempt + 1, "max_retries": max_retries})
            
            try:
                return self._make_traced_instagram_request(encoded_data, headers, data.get('doc_id', 'unknown'))
                    
            except json.JSONDecodeError as e:
                # Track JSON decode errors (empty response / blocked API)
                json_decode_errors += 1
                logger.error(f"JSON Decode Error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                # All retries failed with JSON decode error - API is blocked
                logger.error(f"All {max_retries} attempts failed with JSON decode error. API appears blocked.")
                raise InstagramAPIBlockedException(
                    f"Instagram API returned invalid JSON after {max_retries} attempts. "
                    f"Error: {str(e)}"
                )
            except error.HTTPError as e:
                logger.error(f"HTTP Error {e.code} on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return None
            except error.URLError as e:
                logger.error(f"URL Error: {e} on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except Exception as e:
                # Check if it's a JSON decode error wrapped in another exception
                error_str = str(e)
                if "Expecting value" in error_str or "JSONDecodeError" in error_str:
                    json_decode_errors += 1
                    logger.error(f"JSON-related Error on attempt {attempt + 1}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    # All retries failed with JSON decode error - API is blocked
                    logger.error(f"All {max_retries} attempts failed with JSON-related error. API appears blocked.")
                    raise InstagramAPIBlockedException(
                        f"Instagram API returned invalid response after {max_retries} attempts. "
                        f"Error: {str(e)}"
                    )
                
                logger.error(f"Error: {e} on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        
        return None
    
    def get_media_id(self, shortcode: str) -> Optional[str]:
        """
        Get media ID from shortcode.
        
        Args:
            shortcode: Instagram post shortcode
            
        Returns:
            Media ID or None
        """
        variables = {"shortcode": shortcode}
        data = {
            'variables': json.dumps(variables),
            'doc_id': self.media_info_doc_id
        }
        
        response = self._make_request(data)
        if not response:
            return None
        
        try:
            items = response.get('data', {}).get(
                'xdt_api__v1__media__shortcode__web_info', {}
            ).get('items', [])
            
            if items:
                return items[0].get('pk')
        except (KeyError, TypeError, IndexError) as e:
            logger.error(f"Error extracting media ID: {e}")
        
        return None
    
    def _parse_comment(
        self,
        node: Dict[str, Any],
        job_id: str,
        post_url: str,
        shortcode: str,
        is_reply: bool = False,
        parent_comment_id: str = ''
    ) -> Dict[str, Any]:
        """
        Parse a comment into standardized format.
        
        Args:
            node: Raw comment node from API
            job_id: Job identifier
            post_url: Original post URL
            shortcode: Post shortcode
            is_reply: Whether this is a reply
            parent_comment_id: Parent comment ID (for replies)
            
        Returns:
            Formatted comment dictionary
        """
        user = node.get('user', {})
        comment_id = str(node.get('pk', ''))
        username = user.get('username', '')
        
        return {
            'job_id': job_id,
            'data': {
                'comment_id': comment_id,
                'parent_comment_id': parent_comment_id,
                'platform': 'instagram',
                'type': 'reply' if is_reply else 'comment',
                'text': node.get('text', ''),
                'media': [],
                'profile_image': user.get('profile_pic_url', ''),
                'profile_name': user.get('full_name', username),
                'profile_username': username,
                'profile_meta_data': {
                    'user_id': str(user.get('id', '') or user.get('pk', '')),
                    'is_verified': user.get('is_verified', False)
                },
                'comment_meta_data': {
                    'child_comment_count': node.get('child_comment_count', 0)
                },
                'reply_count': node.get('child_comment_count', 0) if not is_reply else 0,
                'likes_count': node.get('comment_like_count', 0) or node.get('like_count', 0),
                'comment_url': f"https://www.instagram.com/p/{shortcode}/c/{comment_id}/",
                'post_url': post_url,
                'commented_at': self._timestamp_to_iso(node.get('created_at')),
                'scrapped_at': datetime.now(timezone.utc).isoformat()
            }
        }
    
    def _timestamp_to_iso(self, timestamp: Any) -> str:
        """Convert Unix timestamp to ISO format."""
        if not timestamp:
            return ''
        try:
            return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
        except (ValueError, TypeError):
            return str(timestamp)
    
    def fetch_comments_page(
        self,
        media_id: str,
        after_cursor: Optional[str] = None,
        sort_order: str = "popular"
    ) -> Optional[Dict]:
        """
        Fetch a page of comments.
        
        Args:
            media_id: Instagram media ID
            after_cursor: Pagination cursor
            sort_order: 'popular' or 'chronological'
            
        Returns:
            API response or None
        """
        if after_cursor is None:
            variables = {
                "media_id": media_id,
                "__relay_internal__pv__PolarisIsLoggedInrelayprovider": True
            }
        else:
            variables = {
                "after": after_cursor,
                "before": None,
                "first": 50,
                "last": None,
                "media_id": media_id,
                "sort_order": sort_order,
                "__relay_internal__pv__PolarisIsLoggedInrelayprovider": True
            }
        
        data = {
            'variables': json.dumps(variables),
            'doc_id': self.comments_doc_id
        }
        
        return self._make_request(data)
    
    def fetch_replies_page(
        self,
        media_id: str,
        comment_id: str,
        after_cursor: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Fetch a page of replies for a comment.
        
        Args:
            media_id: Instagram media ID
            comment_id: Parent comment ID
            after_cursor: Pagination cursor
            
        Returns:
            API response or None
        """
        if after_cursor is None:
            variables = {
                "after": None,
                "before": None,
                "media_id": media_id,
                "parent_comment_id": comment_id,
                "is_chronological": None,
                "first": None,
                "last": None,
                "__relay_internal__pv__PolarisIsLoggedInrelayprovider": True
            }
        else:
            variables = {
                "after": after_cursor,
                "before": None,
                "media_id": media_id,
                "parent_comment_id": comment_id,
                "is_chronological": None,
                "first": 50,
                "last": None,
                "__relay_internal__pv__PolarisIsLoggedInrelayprovider": True
            }
        
        data = {
            'variables': json.dumps(variables),
            'doc_id': self.replies_doc_id
        }
        
        return self._make_request(data)
    
    @traced_method("instagram.scrape_comments")
    def scrape_comments(
        self,
        post_url: str,
        job_id: str = '',
        max_comments: Optional[int] = None,
        include_replies: bool = True,
        sort_order: str = "popular"
    ) -> Dict[str, Any]:
        """
        Scrape comments from an Instagram post.
        
        Args:
            post_url: Instagram post URL or shortcode
            job_id: Job identifier
            max_comments: Maximum comments to fetch
            include_replies: Whether to fetch replies
            sort_order: 'popular' or 'chronological'
            
        Returns:
            Dictionary with comments and metadata
        """
        # Extract shortcode
        shortcode = self.extract_shortcode(post_url)
        if not shortcode:
            raise ValueError(f"Could not extract shortcode from: {post_url}")
        
        logger.info(f"Starting scrape for shortcode: {shortcode}")
        
        # Add span attributes
        add_span_attributes(
            shortcode=shortcode,
            sort_order=sort_order,
            max_comments=max_comments or 0,
            include_replies=include_replies,
        )
        
        # Get media ID
        media_id = self.get_media_id(shortcode)
        if not media_id:
            raise ValueError(f"Could not get media ID for shortcode: {shortcode}")
        
        logger.info(f"Media ID: {media_id}")
        
        # Add media_id to span
        add_span_attributes(media_id=media_id)
        
        # Build post URL if shortcode was passed
        if not post_url.startswith('http'):
            post_url = f"https://www.instagram.com/p/{shortcode}/"
        
        all_comments = []
        seen_comment_ids = set()  # Track unique comment IDs to detect duplicates
        after_cursor = None
        page = 0
        consecutive_empty_pages = 0
        
        # Log start of scraping
        add_span_event("scraping_started", {
            "shortcode": shortcode,
            "media_id": media_id,
            "sort_order": sort_order
        })  # Track consecutive pages with no new comments
        
        while True:
            page += 1
            logger.info(f"Fetching page {page}...")
            
            response = self.fetch_comments_page(media_id, after_cursor, sort_order)
            if not response:
                logger.error("Failed to fetch comments page after retries. Stopping.")
                break
            
            # Extract comments
            try:
                connection = response.get('data', {}).get(
                    'xdt_api__v1__media__media_id__comments__connection', {}
                )
                edges = connection.get('edges', [])
                page_info = connection.get('page_info', {})
                
                logger.info(f"Page {page}: Got {len(edges)} edges from API")
                
                # First extract all comments, then filter duplicates (like original)
                page_comments = []
                for edge in edges:
                    node = edge.get('node', {})
                    comment_id = node.get('pk')
                    if comment_id:
                        page_comments.append({
                            'comment_id': str(comment_id),
                            'node': node
                        })
                
                # Filter out duplicates
                new_comments = []
                duplicate_count = 0
                for item in page_comments:
                    comment_id = item['comment_id']
                    if comment_id not in seen_comment_ids:
                        seen_comment_ids.add(comment_id)
                        new_comments.append(item)
                    else:
                        duplicate_count += 1
                
                logger.info(f"Page {page}: {len(page_comments)} comments ({duplicate_count} duplicates, {len(new_comments)} new) Total: {len(all_comments)}")
                
                # Process new comments
                for item in new_comments:
                    node = item['node']
                    comment = self._parse_comment(
                        node, job_id, post_url, shortcode,
                        is_reply=False
                    )
                    all_comments.append(comment)
                    
                    # Fetch replies if enabled
                    if include_replies and node.get('child_comment_count', 0) > 0:
                        replies = self._fetch_all_replies(
                            media_id,
                            item['comment_id'],
                            job_id,
                            post_url,
                            shortcode,
                            comment['data']['comment_id']
                        )
                        all_comments.extend(replies)
                    
                    # Check max limit
                    if max_comments and len(all_comments) >= max_comments:
                        logger.info(f"Reached max_comments limit: {max_comments}")
                        break
                
                # Track consecutive empty pages
                if len(new_comments) == 0:
                    consecutive_empty_pages += 1
                    logger.warning(f"No new comments on page {page} ({consecutive_empty_pages} consecutive empty pages)")
                else:
                    consecutive_empty_pages = 0  # Reset counter if we got comments
                
                # Stop if we got 3 consecutive empty pages (Instagram API quirk)
                if consecutive_empty_pages >= 3:
                    logger.info(f"Stopping: {consecutive_empty_pages} consecutive pages with no new comments. Likely end of comments.")
                    break
                
                # Check max limit again after processing
                if max_comments and len(all_comments) >= max_comments:
                    break
                
                # Check pagination
                if not page_info.get('has_next_page'):
                    logger.info("No more pages (has_next_page=False). Fetching complete!")
                    break
                
                after_cursor = page_info.get('end_cursor')
                if not after_cursor:
                    logger.info("No end_cursor. Fetching complete!")
                    break
                
                time.sleep(1.0)  # Rate limiting - increased delay
                
            except (KeyError, TypeError) as e:
                logger.error(f"Error parsing comments: {e}")
                break
        
        
        # Calculate stats
        top_level = len([c for c in all_comments if c['data']['type'] == 'comment'])
        replies = len([c for c in all_comments if c['data']['type'] == 'reply'])
        
        logger.info(f"Scrape complete: {len(all_comments)} total ({top_level} comments, {replies} replies)")
        
        # Add final stats to span
        add_span_attributes(
            total_comments=len(all_comments),
            top_level_comments=top_level,
            reply_comments=replies,
            pages_fetched=page,
        )
        
        return {
            'shortcode': shortcode,
            'media_id': media_id,
            'post_url': post_url,
            'total_comments': len(all_comments),
            'top_level_comments': top_level,
            'reply_comments': replies,
            'comments': all_comments
        }
    
    def _fetch_all_replies(
        self,
        media_id: str,
        comment_id: str,
        job_id: str,
        post_url: str,
        shortcode: str,
        parent_comment_id: str
    ) -> List[Dict]:
        """
        Fetch all replies for a comment.
        
        Args:
            media_id: Media ID
            comment_id: Comment ID to fetch replies for
            job_id: Job identifier
            post_url: Post URL
            shortcode: Post shortcode
            parent_comment_id: Parent comment ID for response
            
        Returns:
            List of reply comments
        """
        replies = []
        seen_reply_ids = set()  # Track unique reply IDs
        after_cursor = None
        previous_cursor = None
        page = 0
        
        while True:
            page += 1
            response = self.fetch_replies_page(media_id, comment_id, after_cursor)
            if not response:
                break
            
            try:
                connection = response.get('data', {}).get(
                    'xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection', {}
                )
                edges = connection.get('edges', [])
                page_info = connection.get('page_info', {})
                
                # Filter out duplicate replies
                new_replies = []
                for edge in edges:
                    node = edge.get('node', {})
                    reply_id = str(node.get('pk', '') or node.get('id', ''))
                    
                    if reply_id and reply_id not in seen_reply_ids:
                        seen_reply_ids.add(reply_id)
                        reply = self._parse_comment(
                            node, job_id, post_url, shortcode,
                            is_reply=True,
                            parent_comment_id=parent_comment_id
                        )
                        new_replies.append(reply)
                
                if not new_replies:
                    break
                
                replies.extend(new_replies)
                
                if not page_info.get('has_next_page'):
                    break
                
                next_cursor = page_info.get('end_cursor')
                if not next_cursor:
                    break
                
                # Check for duplicate cursor (API bug)
                if next_cursor == previous_cursor or next_cursor == after_cursor:
                    break
                
                previous_cursor = after_cursor
                after_cursor = next_cursor
                
                time.sleep(0.1)  # Rate limiting
                
            except (KeyError, TypeError) as e:
                logger.error(f"Error parsing replies: {e}")
                break
        
        return replies


class InstagramHikerCommentScraper:
    """
    Instagram comment scraper using Hiker API (instagrapi).
    Used as a fallback when GraphQL API retries are exhausted.
    
    API Documentation: https://api.instagrapi.com/docs
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize the scraper with Hiker API key.
        
        Args:
            api_key: Hiker API key (defaults to environment variable or hardcoded key)
        """
        self.api_key = api_key or os.environ.get('HIKER_API_KEY', DEFAULT_HIKER_API_KEY)
        self.base_url = "https://api.instagrapi.com"
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers for Hiker API."""
        return {
            'x-access-key': self.api_key,
            'Accept': 'application/json',
            'User-Agent': 'Instagram-Hiker-Lambda-Scraper/1.0'
        }
    
    def extract_shortcode(self, url_or_shortcode: str) -> Optional[str]:
        """
        Extract shortcode from Instagram URL.
        
        Supported formats:
            - https://www.instagram.com/p/SHORTCODE/
            - https://www.instagram.com/reel/SHORTCODE/
            - https://www.instagram.com/reels/SHORTCODE/
            - SHORTCODE (directly)
        """
        # Pattern for URL (supports /p/, /reel/, and /reels/)
        pattern = r'instagram\.com/(?:p|reels?)/([A-Za-z0-9_-]+)'
        match = re.search(pattern, url_or_shortcode)
        if match:
            return match.group(1)
        
        if re.match(r'^[A-Za-z0-9_-]{6,}$', url_or_shortcode):
            return url_or_shortcode
        
        return None
    
    def _make_traced_request(self, url: str, headers: Dict[str, str], endpoint: str, method: str = 'GET') -> Optional[Dict]:
        """Make a traced HTTP request with proper span handling."""
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            f"{method} {endpoint}",
            kind=trace.SpanKind.CLIENT
        ) as http_span:
            http_span.set_attribute("http.method", method)
            http_span.set_attribute("http.url", url)
            http_span.set_attribute("http.scheme", "https")
            http_span.set_attribute("api.provider", "hiker")
            http_span.set_attribute("api.endpoint", endpoint)
            
            try:
                req = request.Request(url, headers=headers, method=method)
                with request.urlopen(req, timeout=60) as response:
                    http_span.set_attribute("http.status_code", response.status)
                    result = json.loads(response.read().decode('utf-8'))
                    http_span.set_status(trace.Status(trace.StatusCode.OK))
                    return result
            except Exception as req_error:
                http_span.record_exception(req_error)
                http_span.set_status(trace.Status(trace.StatusCode.ERROR, str(req_error)))
                raise
    
    @traced_method("hiker_api.request")
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None, max_retries: int = 3) -> Optional[Dict]:
        """Make HTTP GET request to Hiker API."""
        url = f"{self.base_url}{endpoint}"
        if params:
            query_string = parse.urlencode(params)
            url = f"{url}?{query_string}"
        
        headers = self._get_headers()
        
        # Add span attributes
        add_span_attributes(
            hiker_endpoint=endpoint,
            hiker_url=url,
            max_retries=max_retries
        )
        
        for attempt in range(max_retries):
            add_span_event(f"hiker_attempt_{attempt + 1}", {"attempt": attempt + 1})
            
            try:
                return self._make_traced_request(url, headers, endpoint, method='GET')
                
            except error.HTTPError as e:
                logger.error(f"Hiker API HTTP Error {e.code} on attempt {attempt + 1}")
                add_span_event("hiker_http_error", {"attempt": attempt + 1, "status_code": e.code})
                if e.code == 429:  # Rate limited
                    wait_time = min(30, 2 ** (attempt + 2))
                    logger.info(f"Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                elif attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                set_span_error(e)
                return None
            except error.URLError as e:
                logger.error(f"Hiker API URL Error: {e} on attempt {attempt + 1}")
                add_span_event("hiker_url_error", {"attempt": attempt + 1, "error": str(e)})
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                set_span_error(e)
                return None
            except Exception as e:
                logger.error(f"Hiker API Error: {e} on attempt {attempt + 1}")
                add_span_event("hiker_error", {"attempt": attempt + 1, "error_type": type(e).__name__})
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                set_span_error(e)
                return None
        
        return None
    
    def get_media_info_by_url(self, url: str) -> Optional[Dict]:
        """Get media info from URL using Hiker API."""
        endpoint = "/v2/media/by/url"
        params = {"url": url}
        
        logger.info(f"[Hiker] Fetching media info from URL: {url}")
        response = self._make_request(endpoint, params)
        if response:
            logger.info(f"[Hiker] Media info response: {json.dumps(response)[:500]}")
            return response
        
        return None
    
    def get_media_pk(self, shortcode: str) -> Optional[str]:
        """Get media PK (ID) from shortcode."""
        url = f"https://www.instagram.com/p/{shortcode}/"
        media_info = self.get_media_info_by_url(url)
        if media_info:
            pk = media_info.get('pk') or media_info.get('id') or media_info.get('media_id')
            if pk:
                logger.info(f"[Hiker] Found media PK: {pk}")
                return str(pk)
        
        # Fallback: convert shortcode to media ID directly
        media_id = self.shortcode_to_media_id(shortcode)
        if media_id:
            logger.info(f"[Hiker] Converted shortcode to media ID: {media_id}")
            return media_id
        
        return None
    
    def shortcode_to_media_id(self, shortcode: str) -> Optional[str]:
        """Convert Instagram shortcode to media ID using base64 decoding."""
        try:
            alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
            media_id = 0
            for char in shortcode:
                media_id = (media_id * 64) + alphabet.index(char)
            return str(media_id)
        except Exception as e:
            logger.error(f"[Hiker] Error converting shortcode to media ID: {e}")
            return None
    
    def _parse_comment(
        self,
        comment_data: Dict[str, Any],
        job_id: str,
        post_url: str,
        shortcode: str,
        is_reply: bool = False,
        parent_comment_id: str = ''
    ) -> Dict[str, Any]:
        """Parse a comment from Hiker API into standardized format."""
        user = comment_data.get('user', {})
        comment_id = str(comment_data.get('pk', '') or comment_data.get('id', ''))
        username = user.get('username', '')
        
        created_at = comment_data.get('created_at') or comment_data.get('created_at_utc')
        if isinstance(created_at, (int, float)):
            created_at_iso = datetime.fromtimestamp(created_at, tz=timezone.utc).isoformat()
        else:
            created_at_iso = str(created_at) if created_at else ''
        
        return {
            'job_id': job_id,
            'data': {
                'comment_id': comment_id,
                'parent_comment_id': parent_comment_id,
                'platform': 'instagram',
                'type': 'reply' if is_reply else 'comment',
                'text': comment_data.get('text', ''),
                'media': [],
                'profile_image': user.get('profile_pic_url', ''),
                'profile_name': user.get('full_name', username),
                'profile_username': username,
                'profile_meta_data': {
                    'user_id': str(user.get('pk', '') or user.get('id', '')),
                    'is_verified': user.get('is_verified', False)
                },
                'comment_meta_data': {
                    'child_comment_count': comment_data.get('child_comment_count', 0)
                },
                'reply_count': comment_data.get('child_comment_count', 0) if not is_reply else 0,
                'likes_count': comment_data.get('comment_like_count', 0) or comment_data.get('like_count', 0),
                'comment_url': f"https://www.instagram.com/p/{shortcode}/c/{comment_id}/",
                'post_url': post_url,
                'commented_at': created_at_iso,
                'scrapped_at': datetime.now(timezone.utc).isoformat()
            }
        }
    
    def fetch_comments(self, media_pk: str, page_id: Optional[str] = None) -> Optional[Dict]:
        """Fetch comments for a media using Hiker API."""
        endpoint = "/v2/media/comments"
        params = {"id": media_pk}
        
        if page_id:
            params["page_id"] = page_id
        
        logger.info(f"[Hiker] Fetching comments: {endpoint} with params: {params}")
        return self._make_request(endpoint, params)
    
    def fetch_comment_replies(
        self,
        media_pk: str,
        comment_pk: str,
        min_id: Optional[str] = None
    ) -> Optional[Dict]:
        """Fetch replies for a specific comment."""
        endpoint = "/v2/media/comments/replies"
        params = {
            "media_id": media_pk,
            "comment_id": comment_pk
        }
        
        if min_id:
            params["min_id"] = min_id
        
        return self._make_request(endpoint, params)
    
    def _fetch_all_replies(
        self,
        media_pk: str,
        comment_pk: str,
        job_id: str,
        post_url: str,
        shortcode: str,
        parent_comment_id: str
    ) -> List[Dict]:
        """Fetch all replies for a comment with pagination."""
        replies = []
        seen_reply_ids = set()
        min_id = None
        max_pages = 50
        
        for page in range(max_pages):
            if page > 0:
                logger.info(f"    [Hiker] REPLIES PAGINATION: Fetching page {page + 1} for comment {comment_pk}")
            
            response = self.fetch_comment_replies(media_pk, comment_pk, min_id)
            if not response:
                break
            
            try:
                comment_list = response if isinstance(response, list) else response.get('comments', [])
                
                if not comment_list:
                    break
                
                new_replies = []
                for comment_data in comment_list:
                    reply_id = str(comment_data.get('pk', '') or comment_data.get('id', ''))
                    
                    if reply_id and reply_id not in seen_reply_ids:
                        seen_reply_ids.add(reply_id)
                        reply = self._parse_comment(
                            comment_data, job_id, post_url, shortcode,
                            is_reply=True,
                            parent_comment_id=parent_comment_id
                        )
                        new_replies.append(reply)
                
                if not new_replies:
                    break
                
                replies.extend(new_replies)
                
                if isinstance(response, dict):
                    next_min_id = response.get('next_min_id') or response.get('next_max_id')
                    if not next_min_id or next_min_id == min_id:
                        break
                    min_id = next_min_id
                else:
                    break
                
                time.sleep(0.2)
                
            except (KeyError, TypeError) as e:
                logger.error(f"[Hiker] Error parsing replies: {e}")
                break
        
        return replies
    
    @traced_method("hiker_api.scrape_comments")
    def scrape_comments(
        self,
        post_url: str,
        job_id: str = '',
        max_comments: Optional[int] = None,
        include_replies: bool = True
    ) -> Dict[str, Any]:
        """Scrape comments from an Instagram post using Hiker API."""
        shortcode = self.extract_shortcode(post_url)
        if not shortcode:
            raise ValueError(f"[Hiker] Could not extract shortcode from: {post_url}")
        
        logger.info(f"[Hiker] Starting scrape for shortcode: {shortcode}")
        
        # Add span attributes
        add_span_attributes(
            shortcode=shortcode,
            max_comments=max_comments or 0,
            include_replies=include_replies,
            api="hiker",
        )
        
        media_pk = self.get_media_pk(shortcode)
        if not media_pk:
            raise ValueError(f"[Hiker] Could not get media PK for shortcode: {shortcode}")
        
        logger.info(f"[Hiker] Media PK: {media_pk}")
        
        # Add media_pk to span
        add_span_attributes(media_pk=media_pk)
        
        if not post_url.startswith('http'):
            post_url = f"https://www.instagram.com/p/{shortcode}/"
        
        all_comments = []
        seen_comment_ids = set()
        page_id = None
        page = 0
        
        while True:
            page += 1
            logger.info(f"[Hiker] PAGINATION: Fetching page {page}")
            
            response = self.fetch_comments(media_pk, page_id)
            if not response:
                logger.error(f"[Hiker] PAGINATION: Failed to fetch page {page}. Stopping.")
                break
            
            try:
                if isinstance(response, dict):
                    inner_response = response.get('response', response)
                    comment_list = inner_response.get('items', []) or inner_response.get('comments', [])
                    next_page_id = response.get('next_page_id')
                else:
                    comment_list = response
                    next_page_id = None
                
                if not comment_list:
                    logger.info("[Hiker] No more comments. Fetching complete!")
                    break
                
                new_comments = []
                duplicate_count = 0
                
                for comment_data in comment_list:
                    comment_id = str(comment_data.get('pk', '') or comment_data.get('id', ''))
                    
                    if comment_id in seen_comment_ids:
                        duplicate_count += 1
                        continue
                    
                    seen_comment_ids.add(comment_id)
                    
                    comment = self._parse_comment(
                        comment_data, job_id, post_url, shortcode,
                        is_reply=False
                    )
                    new_comments.append(comment)
                    all_comments.append(comment)
                    
                    child_count = comment_data.get('child_comment_count', 0)
                    if include_replies and child_count > 0:
                        logger.info(f"  [Hiker] Fetching {child_count} replies for comment {comment_id}...")
                        replies = self._fetch_all_replies(
                            media_pk,
                            comment_id,
                            job_id,
                            post_url,
                            shortcode,
                            comment['data']['comment_id']
                        )
                        all_comments.extend(replies)
                    
                    if max_comments and len(all_comments) >= max_comments:
                        logger.info(f"[Hiker] Reached max_comments limit: {max_comments}")
                        break
                
                logger.info(f"  [Hiker] new_comments: {len(new_comments)}, duplicates: {duplicate_count}, total: {len(all_comments)}")
                
                if max_comments and len(all_comments) >= max_comments:
                    break
                
                if isinstance(response, dict):
                    next_page = response.get('next_page_id')
                    if not next_page or next_page == page_id:
                        logger.info(f"[Hiker] PAGINATION: No next_page_id. Fetching complete!")
                        break
                    page_id = next_page
                else:
                    break
                
                time.sleep(1.0)
                
            except (KeyError, TypeError) as e:
                logger.error(f"[Hiker] Error parsing comments: {e}")
                break
        
        top_level = len([c for c in all_comments if c['data']['type'] == 'comment'])
        replies_count = len([c for c in all_comments if c['data']['type'] == 'reply'])
        
        logger.info(f"[Hiker] SCRAPE COMPLETE: {len(all_comments)} total ({top_level} comments, {replies_count} replies)")
        
        # Add final stats to span
        add_span_attributes(
            total_comments=len(all_comments),
            top_level_comments=top_level,
            reply_comments=replies_count,
            pages_fetched=page,
        )
        
        return {
            'shortcode': shortcode,
            'media_id': media_pk,
            'post_url': post_url,
            'total_comments': len(all_comments),
            'top_level_comments': top_level,
            'reply_comments': replies_count,
            'comments': all_comments,
            'scraper_used': 'hiker_api'
        }


@traced_lambda_handler()
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function.
    
    Args:
        event: Lambda event containing:
            - post_url (required): Instagram post URL or shortcode
            - job_id (optional): Job identifier
            - cookie_id (optional): Cookie ID for tracking
            - callback_url (optional): URL to POST results to
            - cookies_release_url (optional): URL to Post release cookies
            - cookies (required): Instagram session cookies
            - csrf_token (required): CSRF token
            - max_comments (optional): Maximum comments to fetch
            - include_replies (optional): Include replies (default: True)
            - retry_count (optional): Current retry attempt (default: 0)
        context: Lambda context (unused)
    
    Returns:
        API Gateway compatible response
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Extract critical parameters first for cleanup
    job_id = ''
    callback_url = None
    cookies_release_url = None
    cookie_id = None
    retry_triggered = False
    
    try:
        # Handle both direct Lambda invocation and API Gateway
        if 'body' in event:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        else:
            body = event
        
        # Extract ALL critical parameters early
        job_id = body.get('job_id', '')
        callback_url = body.get('callback_url')
        cookies_release_url = body.get('cookies_release_url')
        cookie_id = body.get('cookie_id')
        
        # Add span attributes for request context
        add_span_attributes(
            job_id=job_id,
            has_callback=bool(callback_url),
            has_cookie=bool(cookie_id),
        )
        
        # Extract parameters
        post_url = body.get('post_url')
        if not post_url:
            add_span_attributes(error_type="missing_post_url")
            return create_response(400, {
                'error': 'Missing required parameter: post_url',
                'example': {
                    'post_url': 'https://www.instagram.com/reel/ABC123/',
                    'job_id': 'unique-job-id',
                    'callback_url': 'https://your-api.com/webhook',
                    'cookies': 'your-session-cookies',
                    'csrf_token': 'your-csrf-token',
                    'max_comments': 500,
                    'retry_count': 0
                }
            })
        
        max_comments = body.get('max_comments')
        include_replies = body.get('include_replies', True)
        
        # Get retry count (default to 0 if not present)
        retry_count = body.get('retry_count', 0)
        logger.info(f"Current retry_count: {retry_count}")
        
        # Add more span attributes
        add_span_attributes(
            post_url=post_url,
            retry_count=retry_count,
            max_comments=max_comments or 0,
            include_replies=include_replies,
        )
        
        # Check if we should use Hiker API (retry count exhausted)
        use_hiker_api = retry_count >= MAX_RETRY_COUNT
        
        # Add span attribute for scraper type
        add_span_attributes(scraper_type="hiker_api" if use_hiker_api else "graphql_api")
        
        if use_hiker_api:
            # Use Hiker API as fallback when retry count is exhausted
            logger.info(f"Retry count ({retry_count}) >= MAX_RETRY_COUNT ({MAX_RETRY_COUNT}). Using Hiker API as fallback.")
            add_span_event("hiker_api_fallback", {"retry_count": retry_count})
            
            # Release cookie immediately - we don't need it for Hiker API
            if cookies_release_url and cookie_id:
                release_payload = {
                    'cookie_id': cookie_id,
                    'cookie_success': False,
                    'failure_reason': 'GraphQL retries exhausted - switching to Hiker API'
                }
                post_to_callback(cookies_release_url, release_payload)
                logger.info(f"Released cookie {cookie_id} before using Hiker API")
                # Clear cookie_id so we don't try to release again
                cookie_id = None
            
            api_key = body.get('hiker_api_key') or os.environ.get('HIKER_API_KEY') or DEFAULT_HIKER_API_KEY
            scraper = InstagramHikerCommentScraper(api_key=api_key)
        else:
            # Use GraphQL API (default)
            DEFAULT_COOKIES = (
                "csrftoken=ges5zhAE6fCnWjySM8hdgKeicO2fE29A; "
                "datr=jcOAaBQBIt3YyzNe8rSmIkzi; "
                "ig_did=D8531209-B194-4404-B9C4-A6D928B85583; "
                "mid=aIC9ogALAAH6_w0q7nXQy8ETm_EK; "
                "sessionid=78598815243%3AQTSGfjNDw5nN70%3A17%3AAYjJRG-DvGWw1l6I-XfkglUKQmI_XTQ_FJPRFNQMPQ; "
                "rur=\"RVA\05478598815243\0541796973450:01fe8260aa3c36601dd4068fb5e7c670f46cef145ed3dc0f38c7fc82767a3fe29fb95522\"; "
                "ds_user_id=78598815243"
            )
            DEFAULT_CSRF_TOKEN = "ges5zhAE6fCnWjySM8hdgKeicO2fE29A"
            
            cookies = body.get('cookies') or os.environ.get('INSTAGRAM_COOKIES') or DEFAULT_COOKIES
            csrf_token = body.get('csrf_token') or os.environ.get('INSTAGRAM_CSRF_TOKEN') or DEFAULT_CSRF_TOKEN
            scraper = InstagramCommentScraper(cookies, csrf_token)
        
        # Fetch comments using the selected scraper
        try:
            result = scraper.scrape_comments(
                post_url=post_url,
                job_id=job_id,
                max_comments=max_comments,
                include_replies=include_replies
            )
        except InstagramAPIBlockedException as e:
            # Instagram API is blocked - immediately trigger Hiker API fallback
            logger.warning(f"Instagram API blocked: {str(e)}. Triggering Hiker API fallback immediately.")
            add_span_event("api_blocked", {"error": str(e)[:200]})
            set_span_error(e)
            
            if not use_hiker_api:
                # Release cookie with failure status (suspected bad cookie)
                if cookies_release_url and cookie_id:
                    release_payload = {
                        'cookie_id': cookie_id,
                        'cookie_success': False,
                        'failure_reason': 'Instagram API blocked - possible bad cookie or rate limit'
                    }
                    post_to_callback(cookies_release_url, release_payload)
                    logger.info(f"Released cookie {cookie_id} with failure due to API block")
                
                # Set retry_count to MAX_RETRY_COUNT to trigger Hiker API on next invocation
                body['retry_count'] = MAX_RETRY_COUNT
                retry_triggered = invoke_lambda_retry(body, MAX_RETRY_COUNT - 1)
                
                # Send intermediate callback (but cookie already released)
                if callback_url:
                    callback_payload = {
                        'job_id': job_id,
                        'success': True,
                        'retry_loop': True,
                        'message': 'Instagram API blocked. Cookie released. Retrying with Hiker API.',
                        'comments': []
                    }
                    post_to_callback(callback_url, callback_payload)
                
                return create_response(200, {
                    'success': True,
                    'job_id': job_id,
                    'message': 'Instagram API blocked. Cookie released. Triggered Hiker API fallback.',
                    'data': {
                        'shortcode': scraper.extract_shortcode(post_url),
                        'post_url': post_url,
                        'total_comments': 0,
                        'comments': []
                    },
                    'retry_info': {
                        'retry_count': retry_count,
                        'api_blocked': True,
                        'retry_triggered': retry_triggered,
                        'fallback_to_hiker': True,
                        'cookie_released': True
                    }
                })
            else:
                # Already using Hiker API and it also failed - re-raise
                raise
        
        # Add scraper type to result if not present
        if 'scraper_used' not in result:
            result['scraper_used'] = 'hiker_api' if use_hiker_api else 'graphql_api'
        
        # Get fetched comment count
        fetched_count = result.get('total_comments', 0)
        
        # Add result metrics to span
        add_span_attributes(
            fetched_comments=fetched_count,
            scraper_used=result['scraper_used'],
        )
        
        # Fetch expected comment count from tracker API
        expected_count = fetch_expected_comment_count(post_url)
        
        # Add expected count to span
        if expected_count is not None:
            add_span_attributes(expected_comments=expected_count)
        
        # Check if retry is needed (only if not using Hiker API - no more retries after Hiker)
        if not use_hiker_api and expected_count is not None and should_retry(fetched_count, expected_count, retry_count):
            # Invoke Lambda with incremented retry count
            add_span_event("retry_triggered", {
                "fetched_count": fetched_count,
                "expected_count": expected_count,
                "retry_count": retry_count,
            })
            retry_triggered = invoke_lambda_retry(body, retry_count)
            logger.info(f"Retry invocation {'succeeded' if retry_triggered else 'failed'}")
        
        # Build response
        response_data = {
            'success': True,
            'job_id': job_id,
            'data': result,
            'retry_info': {
                'retry_count': retry_count,
                'expected_comments': expected_count,
                'fetched_comments': fetched_count,
                'threshold_percent': COMMENT_THRESHOLD_PERCENT * 100,
                'retry_triggered': retry_triggered,
                'used_hiker_fallback': use_hiker_api
            }
        }
        
        # ALWAYS send callback and release cookie (unless retry triggered)
        if not retry_triggered:
            ensure_cleanup(
                job_id, callback_url, cookies_release_url, cookie_id,
                result_data=result,
                error_message=None,
                cookie_success=True,  # Success - cookie worked
                failure_reason=None
            )
            response_data['callback_sent'] = True
            response_data['cookie_released'] = True
        
        return create_response(200, response_data)
    
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        set_span_error(e)
        
        # ALWAYS send callback and release cookie on any error
        ensure_cleanup(
            job_id, callback_url, cookies_release_url, cookie_id,
            result_data=None,
            error_message=str(e),
            cookie_success=False,  # Error - mark cookie as failed
            failure_reason=f"Lambda error: {str(e)[:200]}"  # Truncate long errors
        )
        
        error_response = {
            'success': False,
            'job_id': job_id,
            'error': str(e),
            'callback_sent': True,
            'cookie_released': True
        }
        return create_response(500, error_response)
    
    finally:
        # Force flush all spans to ensure they're sent to BetterStack
        # This is critical in Lambda where execution stops after return
        try:
            tracer_provider = trace.get_tracer_provider()
            if hasattr(tracer_provider, 'force_flush'):
                tracer_provider.force_flush(timeout_millis=2000)
                logger.info("Flushed all spans to BetterStack")
        except Exception as flush_error:
            logger.warning(f"Failed to flush spans: {flush_error}")


def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create API Gateway compatible response.
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'POST, OPTIONS'
        },
        'body': body
    }


def _make_traced_callback_request(callback_url: str, payload: bytes) -> None:
    """Make a traced HTTP POST request to callback URL.
    
    URLLibInstrumentor will automatically:
    1. Create a CLIENT span for this request
    2. Inject trace context (traceparent) into headers
    3. Link the CLIENT span to the current active span
    """
    try:
        # Create request with basic headers
        # URLLibInstrumentor will automatically inject traceparent
        req = request.Request(
            callback_url,
            data=payload,
            method='POST',
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Instagram-Lambda-Scraper/1.0'
            }
        )
        
        # URLLibInstrumentor will automatically create a CLIENT span and inject trace context
        with request.urlopen(req, timeout=30) as response:
            logger.info(f"Callback POST successful: {response.status} to {callback_url}")
            return response.status
            
    except Exception as req_error:
        logger.error(f"Callback POST failed: {str(req_error)}")
        raise


@traced("callback.send", kind=trace.SpanKind.INTERNAL)
def post_to_callback(callback_url: str, data: Any) -> bool:
    """
    Post the response data to a callback URL.
    """
    from opentelemetry import trace as otel_trace
    
    # Log current span for debugging
    current_span = otel_trace.get_current_span()
    if current_span:
        ctx = current_span.get_span_context()
        logger.info(f"post_to_callback executing in trace: {ctx.trace_id:032x}, span: {ctx.span_id:016x}")
    
    add_span_attributes(
        callback_url=callback_url,
        payload_size=len(json.dumps(data, default=str))
    )
    
    try:
        payload = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
        _make_traced_callback_request(callback_url, payload)
        return True
            
    except error.HTTPError as e:
        logger.error(f"Callback POST failed with HTTP {e.code}")
        add_span_event("callback_http_error", {"status_code": e.code})
        set_span_error(e)
        return False
    except error.URLError as e:
        logger.error(f"Callback POST failed with network error: {str(e)}")
        add_span_event("callback_url_error", {"error": str(e)})
        set_span_error(e)
        return False
    except Exception as e:
        logger.error(f"Callback POST failed: {str(e)}")
        add_span_event("callback_unexpected_error", {"error_type": type(e).__name__})
        set_span_error(e)
        return False


@traced("cleanup.ensure_cleanup")
def ensure_cleanup(job_id: str, callback_url: Optional[str], cookies_release_url: Optional[str], 
                   cookie_id: Optional[int], result_data: Optional[Dict] = None, 
                   error_message: Optional[str] = None, cookie_success: bool = True,
                   failure_reason: Optional[str] = None) -> None:
    """
    ALWAYS call callback and release cookie, no matter what happens.
    This ensures jobs and cookies never get stuck.
    
    Args:
        job_id: Job identifier
        callback_url: URL to send results to
        cookies_release_url: URL to release cookie
        cookie_id: Cookie ID to release
        result_data: Success data (if available)
        error_message: Error message (if failed)
        cookie_success: Whether cookie worked successfully (default: True)
        failure_reason: Reason for cookie failure (if cookie_success=False)
    """
    add_span_attributes(
        job_id=job_id,
        has_result=result_data is not None,
        has_error=error_message is not None,
        cookie_success=cookie_success
    )
    
    # Always send callback if URL provided
    if callback_url:
        add_span_event("cleanup_callback_start", {"job_id": job_id})
        if result_data:
            # Success case - send actual results
            callback_payload = {
                'job_id': job_id,
                'success': True,
                'retry_loop': result_data.get('retry_triggered', False),
                'comments': result_data.get('comments', [])
            }
        else:
            # Error case - send error response
            callback_payload = {
                'job_id': job_id,
                'success': False,
                'error': error_message or 'Unknown error',
                'comments': []
            }
        
        callback_success = post_to_callback(callback_url, callback_payload)
        logger.info(f"Cleanup: Callback {'sent' if callback_success else 'failed'} for job {job_id}")
        add_span_event("cleanup_callback_complete", {
            "job_id": job_id,
            "success": callback_success
        })
    
    # Always release cookie if cookie_id provided
    if cookies_release_url and cookie_id:
        add_span_event("cleanup_cookie_release_start", {"cookie_id": cookie_id})
        release_payload = {
            'cookie_id': cookie_id,
            'cookie_success': cookie_success,
            'failure_reason': failure_reason
        }
        release_success = post_to_callback(cookies_release_url, release_payload)
        logger.info(f"Cleanup: Cookie {cookie_id} {'released' if release_success else 'release failed'} (success={cookie_success})")
        add_span_event("cleanup_cookie_release_complete", {
            "cookie_id": cookie_id,
            "success": release_success
        })


# ============================================================
# LOCAL TESTING
# ============================================================

if __name__ == '__main__':
    import sys
    
    # Test credentials (replace with your own)
    test_cookies = os.environ.get('INSTAGRAM_COOKIES', '')
    test_csrf = os.environ.get('INSTAGRAM_CSRF_TOKEN', '')
    
    if not test_cookies or not test_csrf:
        print("\n" + "=" * 60)
        print("ERROR: Instagram credentials not set")
        print("=" * 60)
        print("\nSet environment variables:")
        print("  INSTAGRAM_COOKIES=your-cookies")
        print("  INSTAGRAM_CSRF_TOKEN=your-csrf-token")
        print("=" * 60 + "\n")
        sys.exit(1)
    
    # Test URL
    test_url = "https://www.instagram.com/reel/ABC123/"
    if len(sys.argv) > 1:
        test_url = sys.argv[1]
    
    print(f"\n{'=' * 60}")
    print(f"Testing Instagram Comment Scraper")
    print(f"{'=' * 60}")
    print(f"Post URL: {test_url}")
    print(f"{'=' * 60}\n")
    
    # Simulate Lambda invocation
    test_event = {
        'post_url': test_url,
        'job_id': 'test-job-123',
        'max_comments': 10,
        'cookies': test_cookies,
        'csrf_token': test_csrf
    }
    
    result = lambda_handler(test_event, None)
    
    print(f"\n{'=' * 60}")
    print(f"Result: Status {result['statusCode']}")
    print(f"{'=' * 60}")
    
    response_body = result['body']
    
    if result['statusCode'] == 200:
        data = response_body.get('data', {})
        print(f"✅ Success!")
        print(f"Shortcode: {data.get('shortcode')}")
        print(f"Total Comments: {data.get('total_comments')}")
        print(f"Top-level: {data.get('top_level_comments')}")
        print(f"Replies: {data.get('reply_comments')}")
        
        comments = data.get('comments', [])[:3]
        print(f"\nFirst {len(comments)} comments:")
        for i, comment in enumerate(comments, 1):
            c = comment.get('data', {})
            print(f"\n[{i}] @{c['profile_username']} ({c['likes_count']} likes)")
            text = c['text'][:80] + "..." if len(c['text']) > 80 else c['text']
            print(f"    {text}")
    else:
        print(f"❌ Error: {response_body.get('error')}")
    
    print(f"\n{'=' * 60}\n")

