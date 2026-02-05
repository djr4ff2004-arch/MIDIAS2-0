"""
Uploader module
Handles uploading media to Telegram channels with rate limiting
"""

import os
import logging
import asyncio
import random
from typing import List, Optional, Set
from telegram import Bot, InputMediaPhoto, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, RetryAfter, TimedOut
from telegram.constants import ParseMode
from fetcher import MediaItem
from config import config

logger = logging.getLogger(__name__)


class TelegramUploader:
    """Handles uploading media to Telegram channels"""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.vip_message_ids: List[int] = []  # Store VIP message IDs for forwarding

        # Optional hook for preview indexing. The main application can assign
        # an instance with an `add_vip_message(...)` method.
        self.preview_index = None
    
    async def upload_to_vip(self, media_items: List[MediaItem], 
                           progress_callback=None) -> int:
        """
        Upload media items to VIP channel and store message IDs
        
        Args:
            media_items: List of MediaItem objects with local_path set
            progress_callback: Optional callback for progress updates
        
        Returns:
            Number of successfully uploaded items
        """
        if not config.VIP_CHANNEL_ID:
            logger.error("VIP channel ID not configured")
            return 0
        
        uploaded_count = 0
        total = len(media_items)
        batch_size = config.MAX_FILES_PER_BATCH
        
        # Split into batches
        for i in range(0, total, batch_size):
            batch = media_items[i:i + batch_size]
            
            try:
                # Upload batch and get message IDs
                message_ids = await self._upload_batch(
                    config.VIP_CHANNEL_ID,
                    batch,
                    caption="🔥 Conteúdo VIP"
                )
                
                if message_ids:
                    uploaded_count += len(batch)
                    self.vip_message_ids.extend(message_ids)
                
                if progress_callback:
                    await progress_callback(i + len(batch), total)
                
                # Rate limiting between batches
                await asyncio.sleep(2)
            
            except Exception as e:
                logger.error(f"Error uploading batch to VIP: {e}")
        
        logger.info(f"Uploaded {uploaded_count}/{total} items to VIP channel")
        logger.info(f"Stored {len(self.vip_message_ids)} VIP message IDs for previews")
        return uploaded_count
    
    async def _upload_batch(self, channel_id: int, media_items: List[MediaItem],
                           caption: str = "") -> List[int]:
        """
        Upload a batch of media items to a channel
        
        Args:
            channel_id: Telegram channel ID
            media_items: List of MediaItem objects
            caption: Caption for the media group
        
        Returns:
            List of message IDs if successful, empty list otherwise
        """
        if not media_items:
            return []
        
        try:
            # If single item, send directly
            if len(media_items) == 1:
                msg_id = await self._upload_single(channel_id, media_items[0], caption)
                return [msg_id] if msg_id else []
            
            # Multiple items: create media group
            media_group = []
            
            for item in media_items:
                if not item.local_path or not os.path.exists(item.local_path):
                    logger.warning(f"File not found: {item.local_path}")
                    continue
                
                with open(item.local_path, 'rb') as f:
                    if item.media_type == "video":
                        media_group.append(InputMediaVideo(f))
                    else:
                        media_group.append(InputMediaPhoto(f))
            
            if not media_group:
                return []
            
            # Add caption to first item
            if media_group and caption:
                media_group[0].caption = caption
                media_group[0].parse_mode = ParseMode.MARKDOWN
            
            # Send media group with retry logic
            messages = await self._send_with_retry(
                lambda: self.bot.send_media_group(
                    chat_id=channel_id,
                    media=media_group
                )
            )
            
            # Extract message IDs
            message_ids = [msg.message_id for msg in messages] if messages else []
            return message_ids
        
        except Exception as e:
            logger.error(f"Error uploading batch: {e}")
            return []
    
    async def _upload_single(self, channel_id: int, media_item: MediaItem,
                            caption: str = "") -> Optional[int]:
        """
        Upload a single media item
        
        Args:
            channel_id: Telegram channel ID
            media_item: MediaItem object
            caption: Caption for the media
        
        Returns:
            Message ID if successful, None otherwise
        """
        if not media_item.local_path or not os.path.exists(media_item.local_path):
            logger.warning(f"File not found: {media_item.local_path}")
            return None
        
        try:
            # IMPORTANT: Telegram errors can happen after the file handle has
            # been read once (e.g. 413). If we retry with the same file object,
            # Telegram can respond with "File must be non-empty".
            # To avoid this, we reopen the file on every retry.
            async def _send_once():
                with open(media_item.local_path, 'rb') as f:
                    if media_item.media_type == "video":
                        return await self.bot.send_video(
                            chat_id=channel_id,
                            video=f,
                            caption=caption,
                            parse_mode=None,
                        )
                    return await self.bot.send_photo(
                        chat_id=channel_id,
                        photo=f,
                        caption=caption,
                        parse_mode=None,
                    )

            msg = await self._send_with_retry(_send_once)
            return msg.message_id if msg else None
        
        except Exception as e:
            logger.error(f"Error uploading single item: {e}")
            return None

    async def send_link_fallback(
        self,
        channel_id: int,
        model_name: str,
        url: str,
        size_bytes: int,
        max_bytes: int,
        filename: Optional[str] = None,
    ) -> Optional[int]:
        """When a media file is too large for Telegram (413), send a safe fallback.

        This avoids breaking the whole "Baixar tudo" flow.
        """
        try:
            size_mb = round(size_bytes / (1024 * 1024), 1) if size_bytes else None
            lim_mb = round(max_bytes / (1024 * 1024), 1) if max_bytes else None
            name_part = f"\n📦 <b>{filename}</b>" if filename else ""
            size_part = ""
            if size_mb is not None and lim_mb is not None:
                size_part = f"\n📏 {size_mb}MB (limite bot: {lim_mb}MB)"
            text = (
                f"⚠️ <b>Arquivo muito grande para enviar pelo Telegram.</b>\n"
                f"👤 <b>{model_name}</b>"
                f"{name_part}"
                f"{size_part}\n"
                f"🔗 Link direto: {url}"
            )
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Abrir arquivo", url=url)]])
            msg = await self.bot.send_message(chat_id=channel_id, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return getattr(msg, "message_id", None)
        except TelegramError as e:
            logger.error(f"Telegram error sending link fallback: {e}")
            return None
        except Exception as e:
            logger.error(f"Error sending link fallback: {e}")
            return None
    
    async def send_previews_from_vip(
        self,
        model_name: str,
        message_ids: list[int] | None = None,
        assets: list[tuple[str, str]] | None = None,
        max_previews: int | None = None,
    ):
        """
        Forward random previews from VIP channel to FREE channels with copy
        
        Args:
            model_name: Name of the model for the caption
            max_previews: Maximum number of previews to send (default from config)
        """
        source_ids = list(message_ids) if message_ids else list(self.vip_message_ids)
        source_assets = list(assets) if assets else []

        if not source_ids and not source_assets:
            logger.warning("No VIP messages/assets to forward as previews")
            return
        
        max_previews = max_previews or config.get_value('PREVIEW_LIMIT', 3)
        
        # Select items (prefer message_id when available, otherwise file_id assets)
        if source_ids:
            preview_count = min(max_previews, len(source_ids))
            selected_ids = random.sample(source_ids, preview_count)
            selected_assets: list[tuple[str, str]] = []
        else:
            preview_count = min(max_previews, len(source_assets))
            selected_assets = random.sample(source_assets, preview_count)
            selected_ids = []

        logger.info(f"Sending {preview_count} random previews (ids={len(selected_ids)}, assets={len(selected_assets)})")
        
        # Get FREE channel IDs
        free_channels = {
            'pt': config.get_value("FREE_CHANNEL_PT_ID"),
            'es': config.get_value("FREE_CHANNEL_ES_ID"),
            'en': config.get_value("FREE_CHANNEL_EN_ID")
        }
        
        # Forward to each FREE channel
        for lang, channel_id in free_channels.items():
            if not channel_id:
                continue
            
            try:
                # Get subscription bot link for this language
                sub_link = config.get_sub_link_by_lang(lang)
                
                # Create caption with copy
                caption = self._get_preview_caption(model_name, lang, sub_link)
                
                reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("✅ ASSINAR VIP", url=sub_link)]])

                # 1) Send by VIP message_id (requires VIP_CHANNEL_ID)
                if selected_ids:
                    for msg_id in selected_ids:
                        try:
                            if not config.VIP_CHANNEL_ID:
                                raise ValueError("VIP_CHANNEL_ID not configured")
                            await self.bot.forward_message(
                                chat_id=channel_id,
                                from_chat_id=config.VIP_CHANNEL_ID,
                                message_id=msg_id,
                            )
                            await self.bot.send_message(
                                chat_id=channel_id,
                                text=caption,
                                parse_mode=ParseMode.HTML,
                                reply_markup=reply_markup,
                                disable_web_page_preview=True,
                            )
                            await asyncio.sleep(1)
                        except Exception as e:
                            logger.error(f"Error forwarding message {msg_id} to {lang}: {e}")

                # 2) Send by file_id asset (works from any origin)
                if selected_assets:
                    for media_type, file_id in selected_assets:
                        try:
                            if media_type == "photo":
                                await self.bot.send_photo(chat_id=channel_id, photo=file_id)
                            elif media_type == "video":
                                await self.bot.send_video(chat_id=channel_id, video=file_id)
                            elif media_type == "animation":
                                await self.bot.send_animation(chat_id=channel_id, animation=file_id)
                            elif media_type == "document":
                                await self.bot.send_document(chat_id=channel_id, document=file_id)
                            else:
                                logger.warning(f"Unknown media_type={media_type}, skipping")
                                continue

                            await self.bot.send_message(
                                chat_id=channel_id,
                                text=caption,
                                parse_mode=ParseMode.HTML,
                                reply_markup=reply_markup,
                                disable_web_page_preview=True,
                            )
                            await asyncio.sleep(1)
                        except Exception as e:
                            logger.error(f"Error sending asset {media_type} to {lang}: {e}")
                
                logger.info(f"Sent {preview_count} previews to FREE {lang.upper()} channel")
            
            except Exception as e:
                logger.error(f"Error sending previews to {lang}: {e}")
        
        # Clear VIP message IDs after forwarding
        self.vip_message_ids.clear()
    
    def _get_preview_caption(self, model_name: str, lang: str, sub_link: str) -> str:
        """Generate preview caption with copy in the specified language.

        We intentionally DO NOT embed the sub_link inside the text using Markdown.
        The link is delivered as an inline button (URL), preventing underscore loss
        and other Markdown escaping issues.
        """
        captions = {
            'pt': (
                f"🔥 <b>Preview Exclusiva - {model_name}</b>\n\n"
                "Quer ver <b>TUDO</b> sem censura?\n\n"
                "✨ O acesso VIP inclui:\n"
                "• Conteúdo completo e sem limites\n"
                "• Atualizações diárias\n"
                "• Milhares de fotos e vídeos\n\n"
                "⚠️ Vagas limitadas!"
            ),
            'es': (
                f"🔥 <b>Vista previa exclusiva - {model_name}</b>\n\n"
                "¿Quieres ver <b>TODO</b> sin censura?\n\n"
                "✨ El acceso VIP incluye:\n"
                "• Contenido completo y sin límites\n"
                "• Actualizaciones diarias\n"
                "• Miles de fotos y videos\n\n"
                "⚠️ ¡Plazas limitadas!"
            ),
            'en': (
                f"🔥 <b>Exclusive Preview - {model_name}</b>\n\n"
                "Want to see <b>EVERYTHING</b> uncensored?\n\n"
                "✨ VIP access includes:\n"
                "• Full content with no limits\n"
                "• Daily updates\n"
                "• Thousands of photos and videos\n\n"
                "⚠️ Limited spots!"
            ),
        }
        return captions.get(lang, captions['pt'])
    
    async def _send_with_retry(self, send_func, max_retries: int = 3):
        """
        Send message with retry logic for rate limiting
        
        Args:
            send_func: Async function to send message
            max_retries: Maximum number of retries
        """
        for attempt in range(max_retries):
            try:
                return await send_func()
            
            except RetryAfter as e:
                # Telegram rate limit hit
                wait_time = e.retry_after + 1
                logger.warning(f"Rate limited. Waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
            
            except TimedOut:
                # Timeout - retry with exponential backoff
                wait_time = 2 ** attempt
                logger.warning(f"Timeout. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            
            except TelegramError as e:
                err_text = str(e)
                logger.error(f"Telegram error: {e}")
                # Non-retriable destination/config errors
                if 'Chat not found' in err_text or 'chat not found' in err_text:
                    raise
                # Non-retriable size errors (avoid retry loops like "File must be non-empty")
                if 'Request Entity Too Large' in err_text or 'request entity too large' in err_text:
                    raise
                if attempt == max_retries - 1:
                    raise
        
        return None
    
    async def upload_and_cleanup(
        self,
        media_item: MediaItem,
        channel_id: int,
        caption: str = "",
        model_name: Optional[str] = None,
    ) -> bool:
        """
        Upload a single media item and delete it immediately after
        
        Args:
            media_item: MediaItem object with local_path
            channel_id: Target channel ID
            caption: Optional caption
        
        Returns:
            True if successful, False otherwise
        """
        msg = None
        local_path = media_item.local_path
        try:
            if not local_path or not os.path.exists(local_path):
                logger.warning(f"File not found: {local_path}")
                return False

            # IMPORTANT: Always reopen the file handle for each attempt.
            async def _send_once():
                with open(local_path, 'rb') as f:
                    if media_item.media_type == "video":
                        return await self.bot.send_video(
                            chat_id=channel_id,
                            video=f,
                            caption=caption,
                            parse_mode=None,
                        )
                    if media_item.media_type == "document":
                        return await self.bot.send_document(
                            chat_id=channel_id,
                            document=f,
                            caption=caption,
                            parse_mode=None,
                        )
                    return await self.bot.send_photo(
                        chat_id=channel_id,
                        photo=f,
                        caption=caption,
                        parse_mode=None,
                    )

            msg = await self._send_with_retry(_send_once)

            if msg:
                # Store message ID if uploading to VIP
                if channel_id == config.VIP_CHANNEL_ID:
                    self.vip_message_ids.append(msg.message_id)

                    # Optional: Index the VIP post for preview re-use.
                    if self.preview_index and model_name:
                        try:
                            media_type = "unknown"
                            file_unique_id = None
                            if getattr(msg, "photo", None):
                                media_type = "photo"
                                file_unique_id = msg.photo[-1].file_unique_id
                            elif getattr(msg, "video", None):
                                media_type = "video"
                                file_unique_id = msg.video.file_unique_id
                            elif getattr(msg, "document", None):
                                media_type = "document"
                                file_unique_id = msg.document.file_unique_id

                            self.preview_index.add_vip_message(
                                vip_chat_id=channel_id,
                                vip_message_id=msg.message_id,
                                model=model_name,
                                media_type=media_type,
                                file_unique_id=file_unique_id,
                                caption=getattr(msg, "caption", None),
                            )
                        except Exception as e:
                            logger.warning(f"Preview index hook failed: {e}")

                return True

            return False

        except TelegramError as e:
            # Robustness: 413 should never break the slot loop.
            err_text = str(e)
            if 'Request Entity Too Large' in err_text or 'request entity too large' in err_text:
                try:
                    size_bytes = os.path.getsize(local_path) if local_path and os.path.exists(local_path) else 0
                except Exception:
                    size_bytes = 0
                try:
                    max_upload_mb = int(os.getenv("MAX_TG_UPLOAD_MB", "45"))
                except Exception:
                    max_upload_mb = 45
                max_bytes = max_upload_mb * 1024 * 1024

                logger.warning(f"Telegram 413 for {media_item.url} ({size_bytes} bytes). Sending link fallback.")
                fallback_id = await self.send_link_fallback(
                    channel_id=channel_id,
                    model_name=(model_name or ""),
                    url=media_item.url,
                    size_bytes=size_bytes,
                    max_bytes=max_bytes,
                    filename=media_item.filename,
                )
                return fallback_id is not None

            logger.error(f"Telegram error in upload_and_cleanup: {e}")
            return False
        except Exception as e:
            logger.error(f"Error in upload_and_cleanup: {e}")
            return False
        finally:
            # Always attempt to delete local temp file to avoid Railway disk bloat.
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except Exception:
                    pass
