"""
Media fetcher module - v2.2 (Correct API Endpoint)
Handles searching and downloading media from Coomer.st
"""

import os
import logging
import asyncio
import aiohttp
import aiofiles
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from config import config

logger = logging.getLogger(__name__)


@dataclass
class TooLargeMedia(Exception):
    """Raised when a media file is larger than the Telegram upload limit."""

    url: str
    filename: str
    media_type: str
    size_bytes: int
    # The configured maximum upload size (bytes). This is used for safe fallbacks.
    max_bytes: int = 0

    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)

# Download directory
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


class MediaItem:
    """Represents a media item"""
    
    def __init__(self, url: str, filename: str, media_type: str = "photo", post_id: str = None):
        self.url = url
        self.filename = filename
        self.media_type = media_type  # photo or video
        self.post_id = post_id
        self.local_path: Optional[str] = None
    
    def __repr__(self):
        return f"MediaItem(url={self.url}, type={self.media_type}, post={self.post_id})"


class MediaFetcher:
    # Global cache shared across instances to avoid re-loading ~220k creators repeatedly
    _GLOBAL_CREATORS_CACHE: list[dict] | None = None
    _GLOBAL_CREATORS_TS: float = 0.0
    _GLOBAL_CREATORS_LOCK: asyncio.Lock = asyncio.Lock()

    """Fetches media from Coomer.st using validated APIs"""
    
    BASE_URL = "https://coomer.st"
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/css',
        }
        self._creators_cache = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=180),
            headers=self.headers
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def _get_creators_list(self) -> List[Dict]:
        # Use a global cache shared across MediaFetcher instances to reduce API load.
        ttl_seconds = int(getattr(config, "CREATORS_CACHE_TTL", 6 * 3600) or 6 * 3600)
        now = asyncio.get_event_loop().time()
        try:
            async with self._GLOBAL_CREATORS_LOCK:
                if (
                    self._GLOBAL_CREATORS_CACHE
                    and (now - float(self._GLOBAL_CREATORS_TS or 0.0)) < ttl_seconds
                ):
                    self._creators_cache = list(self._GLOBAL_CREATORS_CACHE)
                    logger.info(f"Loaded {len(self._creators_cache)} creators from global cache")
                    return self._creators_cache
        except Exception:
            pass

        """Get and cache the full creators list"""
        if self._creators_cache is not None:
            return self._creators_cache
        
        try:
            url = f"{self.BASE_URL}/api/v1/creators"
            async with self.session.get(url) as response:
                if response.status == 200:
                    self._creators_cache = await response.json(content_type=None)
                    # Store in global cache for other instances
                    try:
                        async with self._GLOBAL_CREATORS_LOCK:
                            self._GLOBAL_CREATORS_CACHE = list(self._creators_cache or [])
                            self._GLOBAL_CREATORS_TS = asyncio.get_event_loop().time()
                    except Exception:
                        pass
                    logger.info(f"Loaded {len(self._creators_cache)} creators from API")
                    return self._creators_cache
        except Exception as e:
            logger.error(f"Error fetching creators list: {e}")
        
        return []

    async def find_creator(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Find creator by name with exact match priority.
        Returns creator info with service and id.
        """
        creators = await self._get_creators_list()
        if not creators:
            return None
        
        search_name = model_name.lower().strip()
        
        # Priority 1: Exact match
        for creator in creators:
            if creator.get('name', '').lower() == search_name:
                logger.info(f"Exact match found: {creator.get('name')} ({creator.get('service')})")
                return creator
        
        # Priority 2: Name starts with search term
        for creator in creators:
            if creator.get('name', '').lower().startswith(search_name):
                logger.info(f"Starts-with match found: {creator.get('name')} ({creator.get('service')})")
                return creator
        
        # Priority 3: Search term is contained in name (but be more strict)
        if len(search_name) >= 5:
            for creator in creators:
                if search_name in creator.get('name', '').lower():
                    logger.info(f"Contains match found: {creator.get('name')} ({creator.get('service')})")
                    return creator
        
        logger.warning(f"No creator found for: {model_name}")
        return None

    def _normalize_search_term(self, text: str) -> str:
        """
        Normalize search term by removing spaces, underscores, and special chars.
        Example: "belle delphine" -> "belledelphine"
        """
        import re
        normalized = text.lower().strip()
        # Remove spaces, underscores, hyphens
        normalized = re.sub(r'[\s_-]+', '', normalized)
        return normalized
    
    async def find_all_matching_creators(self, model_name: str) -> List[Dict[str, Any]]:
        """
        Find all creators matching the search term with flexible matching.
        Handles: "belle delphine", "belledelphine", "belle_delphine", "vladislava_661"
        """
        creators = await self._get_creators_list()
        if not creators:
            return []
        
        search_normalized = self._normalize_search_term(model_name)
        matches = []
        
        for creator in creators:
            name = creator.get('name', '').lower()
            name_normalized = self._normalize_search_term(name)
            
            # Match if normalized names are similar
            if (search_normalized == name_normalized or 
                search_normalized in name_normalized or
                name_normalized in search_normalized):
                matches.append(creator)
        
        # Sort by exact match first, then by name length
        matches.sort(key=lambda c: (
            0 if self._normalize_search_term(c.get('name', '')) == search_normalized else 1,
            len(c.get('name', ''))
        ))
        
        return matches[:10]

    def _calculate_post_score(self, post: Dict[str, Any]) -> float:
        """
        Calculate desirability score for a post based on:
        - Content richness (60% weight): More media = more "picante" = PRIORITY
        - Recency (40% weight): Newer posts as secondary criteria
        """
        from datetime import datetime, timezone
        
        score = 0.0
        
        # 1. Content richness score (0-60 points) - PRIMARY PRIORITY
        media_count = 1  # Main file
        attachments = post.get('attachments', [])
        media_count += len(attachments)
        
        # More media = higher score (cap at 15 media for 60 points)
        richness_score = min(60, media_count * 4)
        score += richness_score
        
        # 2. Recency score (0-40 points) - SECONDARY
        try:
            published_str = post.get('published', '')
            if published_str:
                # Parse date and make it timezone-aware if needed
                if published_str.endswith('Z'):
                    published_date = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                else:
                    published_date = datetime.fromisoformat(published_str)
                    if published_date.tzinfo is None:
                        published_date = published_date.replace(tzinfo=timezone.utc)
                
                now = datetime.now(timezone.utc)
                days_old = (now - published_date).days
                
                # Exponential decay: newer = better
                # 0 days = 40 points, 30 days = 20 points, 365 days = ~3 points
                recency_score = 40 * (0.95 ** days_old)
                score += recency_score
        except Exception as e:
            logger.warning(f"Error calculating recency score: {e}")
            score += 15  # Default mid-range score
        
        return score

    async def fetch_posts_paged(self, creator: Dict[str, Any], offset: int = 0, smart_sort: bool = True) -> List[MediaItem]:
        """
        Fetch a single page of posts (50 posts) for a specific creator.
        Uses the CORRECT API endpoint: /api/v1/{service}/user/{user_id}/posts
        
        Args:
            creator: Creator dict with service and id
            offset: Pagination offset
            smart_sort: If True, sorts posts by desirability (recency + content richness)
        """
        media_items = []
        
        service = creator.get('service')
        creator_id = creator.get('id')
        
        if not service or not creator_id:
            logger.error(f"Invalid creator data: {creator}")
            return media_items
        
        try:
            # Primary endpoint + fallbacks (Coomer occasionally returns 400 on /posts for some creators/offsets)
            base_posts = f"{self.BASE_URL}/api/v1/{service}/user/{creator_id}"
            candidate_urls = [
                f"{base_posts}/posts?o={offset}",
            ]
            # Retry from offset 0 (some creators reject high offsets with 400)
            if offset:
                candidate_urls.append(f"{base_posts}/posts?o=0")
            # Legacy endpoint is often more reliable
            candidate_urls.append(f"{base_posts}/posts-legacy?o={offset}")
            if offset:
                candidate_urls.append(f"{base_posts}/posts-legacy?o=0")

            posts = None
            for posts_url in candidate_urls:
                logger.info(f"Fetching posts from: {posts_url}")
                try:
                    async with self.session.get(posts_url) as response:
                        if response.status == 200:
                            posts = await response.json(content_type=None)
                            break
                        if response.status in (400, 403, 404, 429, 500, 502, 503):
                            # Try next candidate endpoint
                            try:
                                body = await response.text()
                                logger.debug(f"Posts API non-200 ({response.status}) body[:200]={body[:200]!r}")
                            except Exception:
                                pass
                            continue
                        logger.error(f"Posts API error: {response.status}")
                        continue
                except Exception as e:
                    logger.warning(f"Posts request failed for {posts_url}: {e!r}")
                    continue

            if posts is None:
                logger.error(f"Posts API: all endpoints failed for {service}/{creator_id} (offset={offset})")
                return media_items
                
                # The endpoint returns a list directly
                if not isinstance(posts, list):
                    logger.error(f"Unexpected response type: {type(posts)}")
                    return media_items
                
                logger.info(f"Received {len(posts)} posts at offset {offset}")
                
                # Smart sorting: prioritize desirable content
                if smart_sort and posts:
                    # Calculate scores for all posts
                    posts_with_scores = [(post, self._calculate_post_score(post)) for post in posts]
                    # Sort by score (descending)
                    posts_with_scores.sort(key=lambda x: x[1], reverse=True)
                    posts = [p[0] for p in posts_with_scores]
                    logger.info(f"Posts sorted by desirability (recency + content richness)")
                
                for post in posts:
                    post_id = post.get('id')
                    post_user = post.get('user')
                    
                    # Validate that the post belongs to the correct user
                    if str(post_user) != str(creator_id):
                        logger.warning(f"Post user mismatch: expected {creator_id}, got {post_user}")
                        continue
                    
                    # Extract main file
                    file_info = post.get('file', {})
                    if file_info and file_info.get('path'):
                        path = file_info['path']
                        media_url = f"{self.BASE_URL}/data{path}"
                        filename = file_info.get('name') or f"{post_id}_main"
                        
                        # Determine media type from extension
                        ext = path.lower().split('.')[-1] if '.' in path else ''
                        media_type = "video" if ext in ['mp4', 'm4v', 'mov', 'webm', 'avi'] else "photo"
                        
                        media_items.append(MediaItem(media_url, filename, media_type, str(post_id)))
                    
                    # Extract attachments
                    for i, attachment in enumerate(post.get('attachments', [])):
                        if attachment.get('path'):
                            path = attachment['path']
                            media_url = f"{self.BASE_URL}/data{path}"
                            filename = attachment.get('name') or f"{post_id}_att{i}"
                            
                            ext = path.lower().split('.')[-1] if '.' in path else ''
                            media_type = "video" if ext in ['mp4', 'm4v', 'mov', 'webm', 'avi'] else "photo"
                            
                            media_items.append(MediaItem(media_url, filename, media_type, str(post_id)))
                
                logger.info(f"Extracted {len(media_items)} media items from page")
                
        except Exception as e:
            logger.error(f"Error fetching posts at offset {offset}: {e}")
        
        return media_items

    async def get_total_posts_count(self, creator: Dict[str, Any]) -> int:
        """Get the total number of posts for a creator by fetching first page"""
        service = creator.get('service')
        creator_id = creator.get('id')
        
        try:
            # Fetch first page to count
            posts_url = f"{self.BASE_URL}/api/v1/{service}/user/{creator_id}/posts?o=0"
            async with self.session.get(posts_url) as response:
                if response.status == 200:
                    posts = await response.json(content_type=None)
                    if isinstance(posts, list):
                        # The API returns 50 posts per page, so we estimate
                        # We'll count as we paginate
                        return len(posts)  # Just return first page count for now
        except Exception as e:
            logger.error(f"Error getting post count: {e}")
        
        return 0

    async def download_media(self, media_item: MediaItem, progress_callback=None) -> bool:
        """Download a single media item with retry logic"""
        max_retries = 3

        # Telegram bots will fail with 413 for large multipart uploads. If we keep
        # downloading huge files, Railway can restart the container (disk/OOM),
        # which looks like "config reset" to the user. So we cap downloads to the
        # same size we intend to upload.
        try:
            max_upload_mb = int(os.getenv("MAX_TG_UPLOAD_MB", "200"))
        except Exception:
            max_upload_mb = 45
        max_upload_bytes = max_upload_mb * 1024 * 1024
        download_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': self.BASE_URL
        }
        
        for attempt in range(max_retries):
            try:
                timestamp = int(asyncio.get_event_loop().time() * 1000)
                # Sanitize filename
                safe_name = "".join([c for c in media_item.filename if c.isalnum() or c in "._-"]).strip()
                if not safe_name:
                    safe_name = f"media_{media_item.post_id}"
                
                # Add extension if missing
                if '.' not in safe_name:
                    ext = media_item.url.split('.')[-1].split('?')[0][:4]
                    safe_name = f"{safe_name}.{ext}"
                
                filename = f"{timestamp}_{safe_name}"
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                
                async with self.session.get(media_item.url, headers=download_headers, allow_redirects=True) as response:
                    if response.status == 404:
                        logger.error(f"File not found (404): {media_item.url}")
                        return False
                    if response.status != 200:
                        raise Exception(f"HTTP Status {response.status}")
                    
                    # If server provides Content-Length, fail fast.
                    cl = response.headers.get("Content-Length")
                    if cl:
                        try:
                            cl_int = int(cl)
                            if cl_int > max_upload_bytes:
                                raise TooLargeMedia(
                                    url=media_item.url,
                                    filename=filename,
                                    media_type=media_item.media_type,
                                    size_bytes=cl_int,
                                    max_bytes=max_upload_bytes,
                                )
                        except ValueError:
                            pass

                    total_size = 0
                    async with aiofiles.open(filepath, 'wb') as f:
                        async for chunk in response.content.iter_chunked(32768):
                            await f.write(chunk)
                            total_size += len(chunk)

                            if total_size > max_upload_bytes:
                                # Abort early: file is too large to upload.
                                # Close file then delete it to avoid disk bloat.
                                await f.flush()
                                raise TooLargeMedia(
                                    url=media_item.url,
                                    filename=filename,
                                    media_type=media_item.media_type,
                                    size_bytes=total_size,
                                    max_bytes=max_upload_bytes,
                                )
                    
                    # Verify file was written
                    if total_size > 0 and os.path.exists(filepath):
                        media_item.local_path = filepath
                        logger.info(f"Downloaded {total_size/1024:.1f}KB: {filename}")
                        return True
                    else:
                        logger.warning(f"Download resulted in empty file: {media_item.url}")
                        return False
                        
            except TooLargeMedia:
                # Don't retry oversized files.
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                    except Exception:
                        pass
                raise
            except Exception as e:
                logger.warning(f"Download attempt {attempt+1} failed for {media_item.url}: {e}")
                await asyncio.sleep(2 ** attempt)
        
        return False

    @staticmethod
    def cleanup_downloads():
        """Clean up downloaded files"""
        if not os.path.exists(DOWNLOAD_DIR):
            return
        for filename in os.listdir(DOWNLOAD_DIR):
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, filename))
            except:
                pass