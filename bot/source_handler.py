"""
Source Handler Module
Handles searching from different sources (Coomer, Picazor)
"""

import logging
from typing import Optional, Dict, Any, List
from fetcher import MediaFetcher, MediaItem
from picazor_fetcher import PicazorFetcher

logger = logging.getLogger(__name__)


class SourceHandler:
    """Handles searching and fetching from multiple sources"""
    
    @staticmethod
    async def search_source(source: str, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Search for a creator in the specified source
        
        Args:
            source: 'coomer' or 'picazor'
            model_name: Name of the model/creator
        
        Returns:
            Creator dict with name, url, and service
        """
        try:
            if source == 'coomer':
                async with MediaFetcher() as fetcher:
                    creator = await fetcher.find_creator(model_name)
                    if creator:
                        creator['source'] = 'coomer'
                    return creator
            
            elif source == 'picazor':
                fetcher = PicazorFetcher()
                creator = await fetcher.find_creator(model_name)
                if creator:
                    creator['source'] = 'picazor'
                return creator
            
            else:
                logger.error(f"Unknown source: {source}")
                return None
        
        except Exception as e:
            logger.error(f"Error searching {source}: {e}")
            return None
    
    @staticmethod
    async def find_all_matching(source: str, model_name: str) -> List[Dict[str, Any]]:
        """
        Find all matching creators in the specified source
        
        Args:
            source: 'coomer' or 'picazor'
            model_name: Name of the model/creator
        
        Returns:
            List of creator dicts
        """
        try:
            if source == 'coomer':
                async with MediaFetcher() as fetcher:
                    matches = await fetcher.find_all_matching_creators(model_name)
                    for m in matches:
                        m['source'] = 'coomer'
                    return matches
            
            elif source == 'picazor':
                # Picazor doesn't have a search API, so we just try the exact name
                fetcher = PicazorFetcher()
                creator = await fetcher.find_creator(model_name)
                if creator:
                    creator['source'] = 'picazor'
                    return [creator]
                return []
            
            else:
                return []
        
        except Exception as e:
            logger.error(f"Error finding matches in {source}: {e}")
            return []
    
    @staticmethod
    async def fetch_posts(source: str, creator: Dict[str, Any], offset: int = 0) -> List[MediaItem]:
        """
        Fetch posts from the specified source
        
        Args:
            source: 'coomer' or 'picazor'
            creator: Creator dict
            offset: Pagination offset
        
        Returns:
            List of MediaItem objects
        """
        try:
            if source == 'coomer':
                async with MediaFetcher() as fetcher:
                    return await fetcher.fetch_posts_paged(creator, offset=offset)
            
            elif source == 'picazor':
                fetcher = PicazorFetcher()
                return await fetcher.fetch_posts_paged(creator, offset=offset)
            
            else:
                return []
        
        except Exception as e:
            logger.error(f"Error fetching posts from {source}: {e}")
            return []
    
    @staticmethod
    async def download_media(source: str, media_item: MediaItem) -> bool:
        """
        Download a media item from the specified source
        
        Args:
            source: 'coomer' or 'picazor'
            media_item: MediaItem object
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if source == 'coomer':
                async with MediaFetcher() as fetcher:
                    return await fetcher.download_media(media_item)
            
            elif source == 'picazor':
                fetcher = PicazorFetcher()
                return await fetcher.download_media(media_item)
            
            else:
                return False
        
        except Exception as e:
            logger.error(f"Error downloading from {source}: {e}")
            return False
