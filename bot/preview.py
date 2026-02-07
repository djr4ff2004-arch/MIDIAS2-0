"""
Preview module
Handles creating preview versions of media (blur, watermark, lowres)
"""

import os
import logging
from typing import Optional
from PIL import Image, ImageFilter, ImageDraw, ImageFont
from fetcher import MediaItem
from config import config

logger = logging.getLogger(__name__)

PREVIEW_DIR = "previews"
os.makedirs(PREVIEW_DIR, exist_ok=True)


class PreviewGenerator:
    """Generates preview versions of media"""
    
    @staticmethod
    def create_preview(media_item: MediaItem) -> Optional[MediaItem]:
        """
        Create a preview version of a media item
        
        Args:
            media_item: Original MediaItem with local_path set
        
        Returns:
            New MediaItem with preview, or None if failed
        """
        if not media_item.local_path or not os.path.exists(media_item.local_path):
            logger.warning(f"Original file not found: {media_item.local_path}")
            return None
        
        # Only process images for now (videos require ffmpeg)
        if media_item.media_type != "photo":
            # For videos, return a placeholder or skip
            logger.info(f"Skipping video preview: {media_item.filename}")
            return None
        
        try:
            preview_type = config.PREVIEW_TYPE
            
            if preview_type == "blur":
                return PreviewGenerator._create_blur_preview(media_item)
            elif preview_type == "watermark":
                return PreviewGenerator._create_watermark_preview(media_item)
            elif preview_type == "lowres":
                return PreviewGenerator._create_lowres_preview(media_item)
            elif preview_type == "none":
                # Just return the original item as preview
                return media_item
            else:
                logger.warning(f"Unknown preview type: {preview_type}")
                return None
        
        except Exception as e:
            logger.error(f"Error creating preview: {e}")
            return None
    
    @staticmethod
    def _create_blur_preview(media_item: MediaItem) -> Optional[MediaItem]:
        """Create a blurred preview"""
        try:
            img = Image.open(media_item.local_path)
            
            # Apply Gaussian blur
            blurred = img.filter(ImageFilter.GaussianBlur(radius=20))
            
            # Save preview
            preview_filename = f"preview_blur_{os.path.basename(media_item.local_path)}"
            preview_path = os.path.join(PREVIEW_DIR, preview_filename)
            blurred.save(preview_path, quality=85, optimize=True)
            
            # Create new MediaItem for preview
            preview_item = MediaItem(
                url=media_item.url,
                filename=preview_filename,
                media_type="photo"
            )
            preview_item.local_path = preview_path
            
            logger.info(f"Created blur preview: {preview_filename}")
            return preview_item
        
        except Exception as e:
            logger.error(f"Error creating blur preview: {e}")
            return None
    
    @staticmethod
    def _create_watermark_preview(media_item: MediaItem) -> Optional[MediaItem]:
        """Create a watermarked preview"""
        try:
            img = Image.open(media_item.local_path)
            
            # Create a copy to draw on
            watermarked = img.copy()
            draw = ImageDraw.Draw(watermarked)
            
            # Calculate watermark position and size
            width, height = watermarked.size
            
            # Try to use a font, fallback to default if not available
            try:
                font_size = int(height * 0.08)
                font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
            except:
                font = ImageFont.load_default()
            
            # Watermark text
            watermark_text = "VIP ONLY"
            
            # Get text bounding box
            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            # Position: center
            x = (width - text_width) // 2
            y = (height - text_height) // 2
            
            # Draw semi-transparent background
            padding = 20
            draw.rectangle(
                [x - padding, y - padding, x + text_width + padding, y + text_height + padding],
                fill=(0, 0, 0, 180)
            )
            
            # Draw text
            draw.text((x, y), watermark_text, fill=(255, 255, 255, 255), font=font)
            
            # Save preview
            preview_filename = f"preview_wm_{os.path.basename(media_item.local_path)}"
            preview_path = os.path.join(PREVIEW_DIR, preview_filename)
            watermarked.save(preview_path, quality=85, optimize=True)
            
            # Create new MediaItem for preview
            preview_item = MediaItem(
                url=media_item.url,
                filename=preview_filename,
                media_type="photo"
            )
            preview_item.local_path = preview_path
            
            logger.info(f"Created watermark preview: {preview_filename}")
            return preview_item
        
        except Exception as e:
            logger.error(f"Error creating watermark preview: {e}")
            return None
    
    @staticmethod
    def _create_lowres_preview(media_item: MediaItem) -> Optional[MediaItem]:
        """Create a low-resolution preview"""
        try:
            img = Image.open(media_item.local_path)
            
            # Calculate new size (reduce to configured quality percentage)
            quality_factor = config.PREVIEW_QUALITY / 100.0
            new_width = int(img.width * quality_factor)
            new_height = int(img.height * quality_factor)
            
            # Resize image
            lowres = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            # Save preview
            preview_filename = f"preview_lowres_{os.path.basename(media_item.local_path)}"
            preview_path = os.path.join(PREVIEW_DIR, preview_filename)
            lowres.save(preview_path, quality=60, optimize=True)
            
            # Create new MediaItem for preview
            preview_item = MediaItem(
                url=media_item.url,
                filename=preview_filename,
                media_type="photo"
            )
            preview_item.local_path = preview_path
            
            logger.info(f"Created lowres preview: {preview_filename}")
            return preview_item
        
        except Exception as e:
            logger.error(f"Error creating lowres preview: {e}")
            return None
    
    @staticmethod
    def cleanup_previews():
        """Clean up preview files"""
        try:
            for filename in os.listdir(PREVIEW_DIR):
                filepath = os.path.join(PREVIEW_DIR, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
            logger.info("Cleaned up preview directory")
        except Exception as e:
            logger.error(f"Error cleaning up previews: {e}")


def test_preview():
    """Test preview generation"""
    # This would need an actual image file to test
    pass


if __name__ == "__main__":
    test_preview()
