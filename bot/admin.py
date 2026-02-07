"""
Admin module for Telegram VIP Bot
Handles all administrative commands and permissions
"""

import logging
import sys
import asyncio
import time

from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import RetryAfter, TelegramError
from config import config
from languages import get_text

logger = logging.getLogger(__name__)


# --- Flood-safe admin replies ---
# Admins often run a burst of setup commands (/setvip, /setfree*, /setsubbot_* etc).
# Telegram will rate-limit bots harshly if we reply too fast. We serialize admin replies
# and enforce a minimum interval per chat to avoid 429 + huge RetryAfter windows.
_ADMIN_SEND_LOCK = asyncio.Lock()
_ADMIN_NEXT_ALLOWED_TS: float = 0.0
_ADMIN_COOLDOWN_UNTIL_TS: float = 0.0
_ADMIN_MIN_INTERVAL_SECONDS: float = 1.10  # conservative to avoid per-chat flood

def _now() -> float:
    return time.monotonic()

async def safe_reply_text(update: Update, text: str, **kwargs) -> None:
    """Reply to admin safely without crashing on RetryAfter."""
    global _ADMIN_NEXT_ALLOWED_TS, _ADMIN_COOLDOWN_UNTIL_TS
    if not update or not update.message:
        return
    kwargs.setdefault("parse_mode", None)
    async with _ADMIN_SEND_LOCK:
        try:
            now = _now()
            if now < _ADMIN_COOLDOWN_UNTIL_TS:
                # Skip sending during global cooldown to avoid repeated 429s.
                logger.warning("Skipping admin reply due to active cooldown")
                return
            wait = _ADMIN_NEXT_ALLOWED_TS - now
            if wait > 0:
                await asyncio.sleep(wait)
            await update.message.reply_text(text, **kwargs)
            _ADMIN_NEXT_ALLOWED_TS = _now() + _ADMIN_MIN_INTERVAL_SECONDS
        except RetryAfter as e:
            ra = float(getattr(e, "retry_after", 0) or 0)
            _ADMIN_COOLDOWN_UNTIL_TS = max(_ADMIN_COOLDOWN_UNTIL_TS, _now() + ra)
            logger.warning("Admin reply hit flood control: retry_after=%s", getattr(e, "retry_after", None))
            # Do not re-raise
            return
        except TelegramError as e:
            logger.warning("Admin reply TelegramError: %s", e)
            return
        except Exception as e:
            logger.warning("Admin reply error: %s", e)
            return



def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id == config.ADMIN_ID


async def admin_only(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Decorator-like function to check admin permissions"""
    if not update.effective_user:
        return False
    
    if not is_admin(update.effective_user.id):
        await safe_reply_text(update, 
            get_text("admin_only", config.DEFAULT_LANG)
        )
        return False
    
    return True


async def cmd_setvip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set VIP channel ID"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setvip <channel_id>")
        return
    
    try:
        channel_id = int(context.args[0])
        config.set_value("VIP_CHANNEL_ID", channel_id)
        
        await safe_reply_text(update, 
            get_text("vip_channel_set", config.DEFAULT_LANG, channel_id=channel_id)
        )
        logger.info(f"VIP channel set to {channel_id}")
    except ValueError:
        await safe_reply_text(update, 
            get_text("invalid_channel", config.DEFAULT_LANG)
        )


async def cmd_setfreept(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set FREE PT channel ID"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setfreept <channel_id>")
        return
    
    try:
        channel_id = int(context.args[0])
        config.set_value("FREE_CHANNEL_PT_ID", channel_id)
        
        await safe_reply_text(update, 
            get_text("free_channel_set", config.DEFAULT_LANG, lang_code="PT", channel_id=channel_id)
        )
        logger.info(f"FREE PT channel set to {channel_id}")
    except ValueError:
        await safe_reply_text(update, 
            get_text("invalid_channel", config.DEFAULT_LANG)
        )


async def cmd_setfreees(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set FREE ES channel ID"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setfreees <channel_id>")
        return
    
    try:
        channel_id = int(context.args[0])
        config.set_value("FREE_CHANNEL_ES_ID", channel_id)
        
        await safe_reply_text(update, 
            get_text("free_channel_set", config.DEFAULT_LANG, lang_code="ES", channel_id=channel_id)
        )
        logger.info(f"FREE ES channel set to {channel_id}")
    except ValueError:
        await safe_reply_text(update, 
            get_text("invalid_channel", config.DEFAULT_LANG)
        )


async def cmd_setfreeen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set FREE EN channel ID"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setfreeen <channel_id>")
        return
    
    try:
        channel_id = int(context.args[0])
        config.set_value("FREE_CHANNEL_EN_ID", channel_id)
        
        await safe_reply_text(update, 
            get_text("free_channel_set", config.DEFAULT_LANG, lang_code="EN", channel_id=channel_id)
        )
        logger.info(f"FREE EN channel set to {channel_id}")
    except ValueError:
        await safe_reply_text(update, 
            get_text("invalid_channel", config.DEFAULT_LANG)
        )


async def cmd_setsubbot_pt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set subscription bot link for PT"""
    if not await admin_only(update, context):
        return
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setsubbot_pt <link>")
        return
    link = context.args[0]
    config.set_value("SUB_BOT_LINK_PT", link)
    await safe_reply_text(update, get_text("sub_bot_set", config.DEFAULT_LANG, link=link))

async def cmd_setsubbot_es(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set subscription bot link for ES"""
    if not await admin_only(update, context):
        return
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setsubbot_es <link>")
        return
    link = context.args[0]
    config.set_value("SUB_BOT_LINK_ES", link)
    await safe_reply_text(update, get_text("sub_bot_set", config.DEFAULT_LANG, link=link))

async def cmd_setsubbot_en(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set subscription bot link for EN"""
    if not await admin_only(update, context):
        return
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setsubbot_en <link>")
        return
    link = context.args[0]
    config.set_value("SUB_BOT_LINK_EN", link)
    await safe_reply_text(update, get_text("sub_bot_set", config.DEFAULT_LANG, link=link))


async def cmd_setsource(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set media sources"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setsource <url1,url2,...>")
        return
    
    sources_str = " ".join(context.args)
    sources = [s.strip() for s in sources_str.split(",")]
    config.set_value("MEDIA_SOURCES", sources)
    
    await safe_reply_text(update, 
        get_text("source_set", config.DEFAULT_LANG, sources=", ".join(sources))
    )
    logger.info(f"Media sources set to {sources}")


async def cmd_setpreview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set preview type"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setpreview <blur|watermark|lowres|none>")
        return
    
    preview_type = context.args[0].lower()
    
    if preview_type not in ["blur", "watermark", "lowres", "none"]:
        await safe_reply_text(update, "❌ Tipo inválido. Use: blur, watermark, lowres ou none")
        return
    
    config.set_value("PREVIEW_TYPE", preview_type)
    
    await safe_reply_text(update, 
        get_text("preview_set", config.DEFAULT_LANG, type=preview_type)
    )
    logger.info(f"Preview type set to {preview_type}")

async def cmd_setpreviewlimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set max previews per model"""
    if not await admin_only(update, context):
        return
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setpreviewlimit <numero>")
        return
    try:
        limit = int(context.args[0])
        config.set_value("PREVIEW_LIMIT", limit)
        await safe_reply_text(update, f"✅ Limite de prévias configurado para: {limit}")
    except ValueError:
        await safe_reply_text(update, "❌ Por favor, insira um número válido.")


async def cmd_setlang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set default language"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /setlang <pt|es|en>")
        return
    
    lang = context.args[0].lower()
    
    if lang not in ["pt", "es", "en"]:
        await safe_reply_text(update, 
            get_text("invalid_lang", config.DEFAULT_LANG)
        )
        return
    
    config.set_value("DEFAULT_LANG", lang)
    
    await safe_reply_text(update, 
        get_text("lang_set", lang, new_lang=lang)
    )
    logger.info(f"Default language set to {lang}")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics"""
    if not await admin_only(update, context):
        return
    
    stats = config.get_stats()
    lang = config.DEFAULT_LANG
    
    message = get_text("stats_title", lang)
    message += get_text("stats_vip", lang, vip=stats["vip_channel"])
    message += get_text("stats_free", lang, 
                       pt=stats["free_channels"]["pt"],
                       es=stats["free_channels"]["es"],
                       en=stats["free_channels"]["en"])
    message += get_text("stats_sources", lang, count=stats["media_sources"])
    message += get_text("stats_preview", lang, type=stats["preview_type"])
    message += f"🖼️ Limite de prévias: {config.get_value('PREVIEW_LIMIT', 3)}\n"
    message += get_text("stats_batch", lang, max=stats["max_batch"])
    message += get_text("stats_interval", lang, interval=stats["auto_post_interval"])
    
    await safe_reply_text(update, message, parse_mode="Markdown")


async def cmd_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restart the bot"""
    if not await admin_only(update, context):
        return
    
    await safe_reply_text(update, "🔄 Reiniciando bot...")
    logger.info("Bot restart requested by admin")
    
    # Exit with code 0 - process manager will restart
    sys.exit(0)


async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add user to authorized list (only main admin)"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /addadmin <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        
        if config.add_authorized_user(user_id):
            await safe_reply_text(update, 
                f"✅ **Usuário Autorizado**\n\n"
                f"ID: `{user_id}`\n"
                f"Este usuário agora pode usar o bot.",
                parse_mode="Markdown"
            )
            logger.info(f"Admin {update.effective_user.id} added user {user_id} to whitelist")
        else:
            await safe_reply_text(update, 
                f"⚠️ Usuário `{user_id}` já está autorizado.",
                parse_mode="Markdown"
            )
    except ValueError:
        await safe_reply_text(update, "❌ ID de usuário inválido. Use apenas números.")


async def cmd_removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove user from authorized list (only main admin)"""
    if not await admin_only(update, context):
        return
    
    if not context.args or len(context.args) < 1:
        await safe_reply_text(update, "❌ Uso: /removeadmin <user_id>")
        return
    
    try:
        user_id = int(context.args[0])
        
        if config.remove_authorized_user(user_id):
            await safe_reply_text(update, 
                f"✅ **Autorização Removida**\n\n"
                f"ID: `{user_id}`\n"
                f"Este usuário não pode mais usar o bot.",
                parse_mode="Markdown"
            )
            logger.info(f"Admin {update.effective_user.id} removed user {user_id} from whitelist")
        else:
            if user_id == config.ADMIN_ID:
                await safe_reply_text(update, 
                    "❌ Não é possível remover o administrador principal."
                )
            else:
                await safe_reply_text(update, 
                    f"⚠️ Usuário `{user_id}` não está na lista de autorizados.",
                    parse_mode="Markdown"
                )
    except ValueError:
        await safe_reply_text(update, "❌ ID de usuário inválido. Use apenas números.")


async def cmd_listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all authorized users (only main admin)"""
    if not await admin_only(update, context):
        return
    
    authorized_users = config.get_authorized_users()
    
    if not authorized_users:
        await safe_reply_text(update, "📋 Nenhum usuário autorizado.")
        return
    
    message = "📋 **Usuários Autorizados**\n\n"
    
    for user_id in authorized_users:
        if user_id == config.ADMIN_ID:
            message += f"• `{user_id}` ⭐ (Admin Principal)\n"
        else:
            message += f"• `{user_id}`\n"
    
    message += f"\n**Total:** {len(authorized_users)} usuário(s)"
    
    await safe_reply_text(update, message, parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message"""
    lang = config.DEFAULT_LANG
    user_id = update.effective_user.id
    
    if is_admin(user_id):
        help_text = get_text("help_admin", lang)
    else:
        help_text = get_text("help_user", lang)
    
    await safe_reply_text(update, help_text, parse_mode="Markdown")
