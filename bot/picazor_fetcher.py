"""
Picazor Fetcher Module
Handles searching and downloading media from Picazor.com using gallery-dl
"""

import os
import logging
import asyncio
import subprocess
import json
from typing import List, Dict, Any, Optional
from fetcher import MediaItem

logger = logging.getLogger(__name__)

# Download directory
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


class PicazorFetcher:
    """Fetches media from Picazor.com using gallery-dl"""
    
    BASE_URL = "https://picazor.com"
    
    def __init__(self):
        pass
    
    async def find_creator(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Find creator on Picazor by constructing the profile URL
        
        Args:
            model_name: Name of the model/creator
        
        Returns:
            Creator dict with name and url
        """
        # Picazor URLs are typically: https://picazor.com/username
        creator_url = f"{self.BASE_URL}/{model_name.lower().strip()}"
        
        # Test if URL is valid by attempting to fetch metadata
        try:
            result = await self._run_gallery_dl_json(creator_url, limit=1)
            if result:
                return {
                    'name': model_name,
                    'url': creator_url,
                    'service': 'picazor'
                }
        except Exception as e:
            logger.error(f"Error checking Picazor creator: {e}")
        
        return None
    
    async def fetch_posts_paged(self, creator: Dict[str, Any], offset: int = 0, limit: int = 50) -> List[MediaItem]:
        """
        Fetch a page of posts from Picazor creator
        
        Args:
            creator: Creator dict with url
            offset: Pagination offset (gallery-dl handles this internally)
            limit: Number of items to fetch
        
        Returns:
            List of MediaItem objects
        """
        media_items = []
        creator_url = creator.get('url')
        
        if not creator_url:
            logger.error("Invalid creator data for Picazor")
            return media_items
        
        try:
            # Use gallery-dl to get file list
            files = await self._run_gallery_dl_json(creator_url, limit=limit, offset=offset)
            
            for file_info in files:
                try:
                    url = file_info.get('url') or file_info.get('file_url')
                    filename = file_info.get('filename') or os.path.basename(url)
                    
                    # Determine media type
                    media_type = "video" if filename.lower().endswith(('.mp4', '.webm', '.mov')) else "photo"
                    
                    media_item = MediaItem(
                        url=url,
                        filename=filename,
                        media_type=media_type,
                        post_id=file_info.get('id', str(hash(url)))
                    )
                    
                    media_items.append(media_item)
                
                except Exception as e:
                    logger.error(f"Error parsing Picazor file info: {e}")
            
            logger.info(f"Fetched {len(media_items)} items from Picazor (offset {offset})")
        
        except Exception as e:
            logger.error(f"Error fetching Picazor posts: {e}")
        
        return media_items
    
    async def _run_gallery_dl_json(self, url: str, limit: int = 50, offset: int = 0) -> List[Dict]:
        """
        Run gallery-dl in JSON mode to get file metadata
        
        Args:
            url: Picazor profile URL
            limit: Maximum number of files to fetch
            offset: Skip first N files
        
        Returns:
            List of file metadata dicts
        """
        try:
            # gallery-dl command with JSON output
            cmd = [
                'gallery-dl',
                '--no-download',  # Don't download, just get metadata
                '--dump-json',    # Output JSON
                '--range', f'{offset+1}-{offset+limit}',  # Pagination
                url
            ]
            
            # Run command asynchronously
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"gallery-dl error: {stderr.decode()}")
                return []
            
            # Parse JSON lines
            files = []
            for line in stdout.decode().strip().split('\n'):
                if line:
                    try:
                        files.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            
            return files
        
        except Exception as e:
            logger.error(f"Error running gallery-dl: {e}")
            return []
    
    async def download_media(self, media_item: MediaItem) -> bool:
        """
        Download a single media item using gallery-dl
        
        Args:
            media_item: MediaItem object with url
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Construct local path
            local_path = os.path.join(DOWNLOAD_DIR, media_item.filename)
            
            # Download using gallery-dl
            cmd = [
                'gallery-dl',
                '--filename', media_item.filename,
                '--destination', DOWNLOAD_DIR,
                media_item.url
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            await process.communicate()
            
            if process.returncode == 0 and os.path.exists(local_path):
                media_item.local_path = local_path
                logger.info(f"Downloaded: {media_item.filename}")
                return True
            else:
                logger.error(f"Failed to download: {media_item.filename}")
                return False
        
        except Exception as e:
            logger.error(f"Error downloading from Picazor: {e}")
            return False
