"""
Main bot module - v2.1
Telegram VIP Media Bot - Main entry point
"""

import logging
import asyncio
import os
import random
import zlib
import sys
import re
import time
from datetime import timedelta
from typing import Optional, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.error import TelegramError, RetryAfter, BadRequest, Forbidden, TimedOut, NetworkError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    filters
)

from config import config
from admin import (
    cmd_setvip, cmd_setfreept, cmd_setfreees, cmd_setfreeen,
    cmd_setsubbot_pt, cmd_setsubbot_es, cmd_setsubbot_en,
    cmd_setsource, cmd_setpreview, cmd_setpreviewlimit, cmd_setlang,
    cmd_stats, cmd_restart, cmd_help,
    cmd_addadmin, cmd_removeadmin, cmd_listadmins
)
from fetcher import MediaFetcher, TooLargeMedia, MediaItem
from uploader import TelegramUploader
from preview import PreviewGenerator
from languages import get_text
from users import user_manager
from referral import ReferralManager
from preview_index import PreviewIndexManager, PreviewIndex

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# Conversation states (broadcast)
BC_TARGET, BC_CONTENT, BC_BTN_Q, BC_BTN_TEXT, BC_BTN_URL, BC_CONFIRM = range(6)

# Conversation states (manual previews)
PV_MENU = 100
PV_SEND_MODEL = 101
PV_INDEX_MODEL = 102
PV_INDEX_COLLECT = 103
PV_RAND_MODEL = 104
PV_RAND_QTY = 105
PV_RAND_DEST = 106


_MODEL_TOKEN_RE = re.compile(r"[a-z0-9_.]+", re.IGNORECASE)


def extract_model_from_caption(text: str) -> str:
    """Extract model token from captions/text.

    Rules:
      - Match "Model:" / "Modelo:" / "model -" etc and extract token [a-z0-9_.]+
      - Match hashtags like #thiccameron
      - Fallback: 'all'
    """
    if not text:
        return "all"

    t = text.strip()

    # 1) Explicit labels
    m = re.search(r"\b(model|modelo)\s*[:\-–]\s*([a-z0-9_.]+)", t, flags=re.IGNORECASE)
    if m:
        return m.group(2).lower()

    # 2) Hashtag
    m = re.search(r"#([a-z0-9_.]+)", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()

    return "all"

def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram Markdown"""
    if not text:
        return text
    # Characters that need to be escaped in Markdown
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


class VIPBot:
    """Main bot class"""
    
    def __init__(self):
        self.app: Application = None
        self.uploader: TelegramUploader = None
        self.search_cache = {}
        # Telegram flood-control guard: if Telegram returns RetryAfter with a long cooldown,
        # stop attempting further send/edit calls until cooldown expires to avoid endless 429 spam.
        # Use time.monotonic() so it keeps working across clock changes.
        self._tg_cooldown_until = 0.0
        self._tg_cooldown_until_per_chat = {}  # chat_id -> monotonic seconds
        self._tg_last_call_per_chat = {}  # chat_id -> monotonic seconds
        self._tg_min_interval_seconds = 1.10  # conservative throttle to reduce flood risk
        # Referral DB
        # ReferralManager reads DATABASE_URL from env internally (if available) and
        # falls back to a local SQLite file. Keep the constructor call compatible.
        self.referrals = ReferralManager(db_path="referrals.db")
        self.preview_index = PreviewIndexManager(database_url=config.DATABASE_URL, db_path="previews_index.db")

        # New robust preview database (auto-index + no-repeat + auto mode).
        self.preview_db = PreviewIndex(database_url=config.DATABASE_URL, sqlite_path="previews_index.db")
        self._auto_previews_lock = asyncio.Lock()
        self._auto_previews_task: asyncio.Task | None = None
        self._auto_previews_stop_event = asyncio.Event()

        # VIP feed module (periodic VIP seeding from Coomer top creators)
        self._vip_feed_lock = asyncio.Lock()
        self._vip_feed_task: asyncio.Task | None = None
        self._vip_feed_stop_event = asyncio.Event()
        self._vip_feed_top_cache = {"ts": 0.0, "creators": []}

    async def check_authorization(self, update: Update, public_ok: bool = False) -> bool:
        """Check if user is authorized to use the bot.

        If public_ok=True and PUBLIC_REFERRAL_MODE is enabled, we allow non-whitelisted users
        to use limited public commands (/start, /ref) without granting full access.
        """
        if not update.effective_user:
            return False

        user_id = update.effective_user.id

        if config.is_authorized(user_id):
            return True

        if public_ok and getattr(config, 'PUBLIC_REFERRAL_MODE', False):
            return True

        if update.message:
            await update.message.reply_text(
                "❌ **Acesso Negado**\n\nEste bot é privado e restrito a usuários autorizados.\n\nSe você acredita que deveria ter acesso, entre em contato com o administrador.",
                parse_mode=None
            )

        logger.warning(f"Unauthorized access attempt by user {user_id}")
        return False



    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command (supports public referral mode)."""
        if not await self.check_authorization(update, public_ok=True):
            return

        user = update.effective_user
        user_manager.get_user(user.id)
        lang = user_manager.get_language(user.id)

        # Record referral if /start used with deep-link: /start ref_<user_id>
        if context.args:
            arg0 = context.args[0]
            if isinstance(arg0, str) and arg0.startswith('ref_'):
                referrer_id = 0
                try:
                    referrer_id = int(arg0.split('_', 1)[1])
                except Exception:
                    referrer_id = 0

                if referrer_id and referrer_id != user.id:
                    recorded = self.referrals.record_referral(referrer_id, user.id)
                    if recorded:
                        # Notify referrer (best effort)
                        try:
                            count = self.referrals.get_referral_count(referrer_id)
                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=get_text('referral_new', user_manager.get_language(referrer_id), count=count),
                            )
                        except Exception:
                            pass
                        # Log to main admin
                        try:
                            await context.bot.send_message(
                                chat_id=config.ADMIN_ID,
                                text=get_text('referral_admin_log', config.DEFAULT_LANG, referrer_id=referrer_id, referred_id=user.id),
                            )
                        except Exception:
                            pass

        # Public vs private welcome
        if (not config.is_authorized(user.id)) and config.PUBLIC_REFERRAL_MODE:
            free_link = {
                'pt': getattr(config, 'FREE_JOIN_LINK_PT', ''),
                'es': getattr(config, 'FREE_JOIN_LINK_ES', ''),
                'en': getattr(config, 'FREE_JOIN_LINK_EN', ''),
            }.get(lang, getattr(config, 'FREE_JOIN_LINK_PT', ''))

            msg = get_text('public_welcome', lang, name=user.first_name)
            if free_link:
                msg = msg + "\n\n🔗 " + free_link
            msg = msg + "\n\n👉 /ref"

            await update.message.reply_text(
                msg,
                disable_web_page_preview=True,
            )
            return

        # Authorized (admin/private) welcome
        welcome = get_text('private_welcome', lang, name=user.first_name)
        reply_markup = None
        # user_id is the Telegram user identifier; use the effective user.
        if user.id == config.ADMIN_ID:
            reply_markup = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📢 Broadcast", callback_data="admin:broadcast"),
                    InlineKeyboardButton("🖼️ Previews", callback_data="admin:previews"),
                ],
                [
                    InlineKeyboardButton("🤖 Modo automático", callback_data="admin:auto"),
                ],
            ])
        await update.message.reply_text(welcome, parse_mode=None, reply_markup=reply_markup)

    # ---------- Manual previews (admin) ----------
    async def pv_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Conversation entry: previews menu (send or index)."""
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            # silent for non-admin
            return ConversationHandler.END

        if update.callback_query:
            await update.callback_query.answer()
            msg = update.callback_query.message
        else:
            msg = update.message

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📤 Enviar para FREE", callback_data="pv:send"),
                InlineKeyboardButton("📌 Indexar do VIP", callback_data="pv:index"),
            ],
            [InlineKeyboardButton("🎲 Previews aleatórias (sem repetir)", callback_data="pv:random")],
            [InlineKeyboardButton("⬅️ Voltar", callback_data="admin:menu")],
        ])

        await msg.reply_text(
            "🖼️ *Previews*\n\n"
            "• *Enviar para FREE*: encaminha previews que já estão indexadas (ou do cache atual).\n"
            "• *Indexar do VIP*: você encaminha mensagens do canal VIP aqui, e eu salvo os `message_id` para usar depois.\n\n"
            "• *Aleatórias (sem repetir)*: escolhe previews aleatórias do VIP, usando CopyMessage, e evita repetição por destino.\n\n"
            "Escolha uma opção:",
            parse_mode=None,
            reply_markup=keyboard,
        )
        return PV_MENU

    async def pv_menu_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        if not update.callback_query:
            return PV_MENU

        await update.callback_query.answer()
        data = update.callback_query.data or ""

        if data == "pv:send":
            await update.callback_query.message.reply_text(
                "📤 Enviar previews para FREE\n\n"
                "Digite o *nome da modelo* (igual aparece no VIP).\n\n"
                "Ex.: `thiccameron`\n\n/cancel para sair.",
                parse_mode=None,
            )
            return PV_SEND_MODEL

        if data == "pv:index":
            await update.callback_query.message.reply_text(
                "📌 Indexar previews do VIP\n\n"
                "1) Digite o *nome da modelo* (igual no VIP).\n"
                "2) Depois, encaminhe aqui (forward) as mensagens do canal VIP dessa modelo.\n"
                "3) Quando terminar, envie /done.\n\n/cancel para sair.",
                parse_mode=None,
            )
            return PV_INDEX_MODEL

        if data == "pv:random":
            # Step 1: model
            context.user_data.pop("pv_rand_model", None)
            context.user_data.pop("pv_rand_qty", None)

            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Todas", callback_data="pv:rand:all")],
                    [InlineKeyboardButton("Cancelar", callback_data="pv:rand:cancel")],
                ]
            )
            await update.callback_query.message.reply_text(
                "🎲 Previews aleatórias (sem repetir)\n\n"
                "Digite o modelo (ex: thiccameron) ou clique em 'Todas'.",
                parse_mode=None,
                reply_markup=keyboard,
            )
            return PV_RAND_MODEL

        if data == "admin:menu":
            # fall back to normal /start menu in private
            await update.callback_query.message.reply_text("OK.")
            return ConversationHandler.END

        return PV_MENU

    async def pv_receive_model(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Receive model name and send previews to FREE from indexed VIP messages (preferred)."""
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        model_name = (update.message.text or "").strip()
        if not model_name:
            await update.message.reply_text("❌ Nome inválido. Digite novamente ou /cancel.")
            return PV_SEND_MODEL

        indexed_ids = self.preview_index.get_message_ids(model_name)
        indexed_assets = self.preview_index.get_assets(model_name)

        # If the admin previously indexed the creator using a slightly different key
        # (e.g., including platform), try to resolve by partial match.
        if not indexed_ids and not indexed_assets:
            candidates = self.preview_index.find_models(model_name, limit=10)
            if len(candidates) == 1:
                model_name = candidates[0]
                indexed_ids = self.preview_index.get_message_ids(model_name)
                indexed_assets = self.preview_index.get_assets(model_name)
            elif len(candidates) > 1:
                suggestions = "\n".join([f"• {c}" for c in candidates[:10]])
                await update.message.reply_text(
                    "⚠️ Encontrei mais de uma modelo parecida no índice. Especifique exatamente uma destas:\n\n" + suggestions
                )
                return ConversationHandler.END

        use_cache = False
        use_assets = False

        if not indexed_ids and indexed_assets:
            use_assets = True

        if not indexed_ids and not use_assets:
            # fallback to last-run cache (only if something was downloaded recently)
            cache_ids = getattr(self.uploader, "vip_message_ids", []) if self.uploader else []
            if cache_ids:
                indexed_ids = list(cache_ids)
                use_cache = True

        if not indexed_ids and not use_assets:
            await update.message.reply_text(
                "⚠️ Não tenho previews indexadas para essa modelo.\n\n"
                "Use *Indexar do VIP* e encaminhe as mídias do VIP aqui (canal ou grupo), ou faça um download recente para popular o cache.",
                parse_mode=None,
            )
            return ConversationHandler.END

        try:
            await update.message.reply_text("⏳ Enviando previews para os canais FREE...")
            await self.uploader.send_previews_from_vip(
                model_name=model_name,
                message_ids=None if use_cache else indexed_ids,
                assets=indexed_assets if use_assets else None,
            )
            await update.message.reply_text("✅ Previews enviadas para os canais FREE.")
        except Exception as e:
            logger.error(f"Manual previews error: {e}")
            await update.message.reply_text(f"⚠️ Erro ao enviar previews: {e}")

        return ConversationHandler.END

    async def pv_index_model(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        model_name = (update.message.text or "").strip()
        if not model_name:
            await update.message.reply_text("❌ Nome inválido. Digite novamente ou /cancel.")
            return PV_INDEX_MODEL

        context.user_data["pv_index_model"] = model_name
        context.user_data["pv_index_count"] = 0

        vip_id = config.VIP_CHANNEL_ID
        await update.message.reply_text(
            f"✅ Modelo definida: *{model_name}*\n\n"
            f"Agora encaminhe (forward) aqui as mídias do VIP (canal ou grupo) dessa modelo.\n"
            f"• Se for do *canal* VIP, o bot salva o *message_id* e consegue copiar depois.\n"
            f"• Se for de *grupo*, às vezes o Telegram não informa a origem: aí eu salvo o *file_id* e também dá pra enviar sem baixar.\n\n"
            f"Canal VIP configurado (opcional): `{vip_id}`\n\n"
            f"Quando terminar, envie /done.",
            parse_mode=None,
            disable_web_page_preview=True,
        )
        return PV_INDEX_COLLECT

    def _extract_forward_source(self, msg):
        """Return (from_chat_id, from_message_id) for forwarded messages."""
        try:
            fc = getattr(msg, "forward_from_chat", None)
            fmid = getattr(msg, "forward_from_message_id", None)
            if fc and fmid:
                return fc.id, int(fmid)
        except Exception:
            pass

        # New Bot API forward_origin (PTB v20+)
        try:
            origin = getattr(msg, "forward_origin", None)
            if origin and hasattr(origin, "chat") and hasattr(origin, "message_id"):
                return origin.chat.id, int(origin.message_id)
        except Exception:
            pass

        return None

    def _extract_media_asset(self, msg: Message) -> Optional[Tuple[str, str]]:
        """Extract (media_type, file_id) from a message.

        Telegram may omit forward origin chat/message_id for messages forwarded
        from groups (the origin becomes the user). In that case we can still
        index previews by storing the media file_id.
        """
        try:
            if getattr(msg, "photo", None):
                return "photo", msg.photo[-1].file_id
            if getattr(msg, "video", None):
                return "video", msg.video.file_id
            if getattr(msg, "animation", None):
                return "animation", msg.animation.file_id
            if getattr(msg, "document", None):
                return "document", msg.document.file_id
        except Exception:
            return None
        return None

    async def pv_index_collect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        model_name = context.user_data.get("pv_index_model")
        if not model_name:
            await update.message.reply_text("⚠️ Estado perdido. Use /previews novamente.")
            return ConversationHandler.END

        # Prefer indexing by chat/message_id (works best for channels).
        src = self._extract_forward_source(update.message)
        indexed_msg_id: Optional[int] = None

        if src:
            from_chat_id, from_message_id = src
            if (not config.VIP_CHANNEL_ID) or (int(from_chat_id) == int(config.VIP_CHANNEL_ID)):
                if self.preview_index.add(model_name, int(from_message_id)):
                    indexed_msg_id = int(from_message_id)
            else:
                # Don't block the admin: we can still index by file_id below.
                logger.info(
                    "Preview forward chat mismatch: got %s expected %s; falling back to file_id",
                    from_chat_id,
                    config.VIP_CHANNEL_ID,
                )

        # Fallback: index by Telegram file_id (works for forwards from groups).
        indexed_asset: Optional[Tuple[str, str]] = None
        if indexed_msg_id is None:
            indexed_asset = self._extract_media_asset(update.message)
            if not indexed_asset:
                await update.message.reply_text(
                    "⚠️ Encaminhe uma mídia (foto/vídeo/documento) do VIP para eu indexar."
                )
                return PV_INDEX_COLLECT
            media_type, file_id = indexed_asset
            self.preview_index.add_asset(model_name, media_type, file_id)

        context.user_data["pv_index_count"] = int(context.user_data.get("pv_index_count", 0)) + 1
        count = context.user_data.get("pv_index_count", 0)

        if indexed_msg_id is not None:
            status = f"📌 Indexado por mensagem: `{indexed_msg_id}`"
        else:
            status = f"📌 Indexado por arquivo: `{indexed_asset[0]}`"

        await update.message.reply_text(
            f"{status} (total nesta sessão: {count})\nEnvie mais ou /done.",
            parse_mode=None,
        )
        return PV_INDEX_COLLECT

    async def pv_index_done(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        model_name = context.user_data.get("pv_index_model")
        count = context.user_data.get("pv_index_count", 0)
        total = self.preview_index.count(model_name) if model_name else 0
        await update.message.reply_text(
            f"✅ Indexação finalizada para `{model_name}`.\n"
            f"Adicionados nesta sessão: {count}. Total salvo: {total}.",
            parse_mode=None,
        )
        return ConversationHandler.END

    # ---------- Random previews (admin) ----------
    async def pv_rand_model_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        text = (update.message.text or "").strip()
        token_match = _MODEL_TOKEN_RE.search(text)
        if not token_match:
            await update.message.reply_text("❌ Modelo inválido. Envie um token como: thiccameron")
            return PV_RAND_MODEL
        model = token_match.group(0).lower()
        context.user_data["pv_rand_model"] = model
        return await self._pv_rand_ask_qty(update, context)

    async def pv_rand_model_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        q = update.callback_query
        await q.answer()
        data = q.data or ""
        if data == "pv:rand:cancel":
            try:
                await q.message.reply_text("Cancelado.", parse_mode=None)
            except Exception:
                pass
            return ConversationHandler.END
        if data == "pv:rand:all":
            context.user_data["pv_rand_model"] = "all"
            return await self._pv_rand_ask_qty(update, context)
        return PV_RAND_MODEL

    async def _pv_rand_ask_qty(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("5", callback_data="pv:qty:5"),
                    InlineKeyboardButton("10", callback_data="pv:qty:10"),
                    InlineKeyboardButton("20", callback_data="pv:qty:20"),
                ],
                [InlineKeyboardButton("Cancelar", callback_data="pv:qty:cancel")],
            ]
        )
        msg = update.effective_message
        await msg.reply_text(
            "Quantos previews enviar? (1-50)",
            parse_mode=None,
            reply_markup=kb,
        )
        return PV_RAND_QTY

    async def pv_rand_qty_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        txt = (update.message.text or "").strip()
        try:
            qty = int(txt)
        except Exception:
            await update.message.reply_text("❌ Quantidade inválida. Envie um número entre 1 e 50.")
            return PV_RAND_QTY
        qty = max(1, min(qty, 50))
        context.user_data["pv_rand_qty"] = qty
        return await self._pv_rand_ask_dest(update, context)

    async def pv_rand_qty_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        q = update.callback_query
        await q.answer()
        data = q.data or ""
        if data == "pv:qty:cancel":
            try:
                await q.message.reply_text("Cancelado.", parse_mode=None)
            except Exception:
                pass
            return ConversationHandler.END
        if data.startswith("pv:qty:"):
            try:
                qty = int(data.split(":")[-1])
            except Exception:
                qty = 5
            qty = max(1, min(qty, 50))
            context.user_data["pv_rand_qty"] = qty
            return await self._pv_rand_ask_dest(update, context)
        return PV_RAND_QTY

    async def _pv_rand_ask_dest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        free_map = {
            "PT": config.get_value("FREE_CHANNEL_PT_ID"),
            "ES": config.get_value("FREE_CHANNEL_ES_ID"),
            "EN": config.get_value("FREE_CHANNEL_EN_ID"),
        }
        rows = [[InlineKeyboardButton("Privado (este chat)", callback_data="pv:dest:private")]]
        rows.append([InlineKeyboardButton("FREE (todos)", callback_data="pv:dest:free:all")])
        # Specific FREE channels (if configured)
        specific_btns = []
        for lbl, cid in free_map.items():
            if cid:
                specific_btns.append(InlineKeyboardButton(f"FREE {lbl}", callback_data=f"pv:dest:free:{cid}"))
        if specific_btns:
            # split into rows of 2
            for i in range(0, len(specific_btns), 2):
                rows.append(specific_btns[i : i + 2])

        rows.append([InlineKeyboardButton("Ambos (privado + todos FREE)", callback_data="pv:dest:both:all")])
        if specific_btns:
            both_btns = []
            for lbl, cid in free_map.items():
                if cid:
                    both_btns.append(InlineKeyboardButton(f"Ambos + FREE {lbl}", callback_data=f"pv:dest:both:{cid}"))
            for i in range(0, len(both_btns), 2):
                rows.append(both_btns[i : i + 2])
        rows.append([InlineKeyboardButton("Cancelar", callback_data="pv:dest:cancel")])
        kb = InlineKeyboardMarkup(rows)
        msg = update.effective_message
        await msg.reply_text("Destino?", parse_mode=None, reply_markup=kb)
        return PV_RAND_DEST

    async def pv_rand_dest_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END

        q = update.callback_query
        await q.answer()
        data = q.data or ""
        if data == "pv:dest:cancel":
            try:
                await q.message.reply_text("Cancelado.", parse_mode=None)
            except Exception:
                pass
            return ConversationHandler.END

        model = context.user_data.get("pv_rand_model", "all")
        qty = int(context.user_data.get("pv_rand_qty", 5))
        private_chat_id = update.effective_chat.id if update.effective_chat else None

        targets: list[int] = []

        parts = data.split(":")
        # data formats:
        # pv:dest:private
        # pv:dest:free:all | pv:dest:free:<chat_id>
        # pv:dest:both:all | pv:dest:both:<chat_id>
        mode = parts[2] if len(parts) >= 3 else ""
        arg = parts[3] if len(parts) >= 4 else ""

        if mode in ("private", "both") and private_chat_id:
            targets.append(int(private_chat_id))

        if mode in ("free", "both"):
            if arg and arg != "all":
                try:
                    targets.append(int(arg))
                except Exception:
                    targets.extend(self._get_free_channel_ids())
            else:
                targets.extend(self._get_free_channel_ids())

        if not targets:
            await q.message.reply_text("⚠️ Nenhum destino configurado.", parse_mode=None)
            return ConversationHandler.END

        sent_total, err_total = await self._send_random_previews(
            bot=context.bot,
            model=str(model),
            qty=int(qty),
            dest_chat_ids=targets,
        )

        await q.message.reply_text(
            f"✅ Concluído. Enviados: {sent_total}. Erros: {err_total}.",
            parse_mode=None,
        )
        return ConversationHandler.END

    def _get_free_channel_ids(self) -> list[int]:
        ids: list[int] = []
        for attr in ("FREE_CHANNEL_PT_ID", "FREE_CHANNEL_ES_ID", "FREE_CHANNEL_EN_ID"):
            cid = getattr(config, attr, None)
            if cid:
                try:
                    ids.append(int(cid))
                except Exception:
                    continue
        # Deduplicate while preserving order
        seen = set()
        out: list[int] = []
        for i in ids:
            if i not in seen:
                seen.add(i)
                out.append(i)
        return out


    def _coomer_item_key(self, creator: dict, media_item: MediaItem) -> int:
        """Stable int key for a Coomer media item (used for no-repeat)."""
        try:
            service = str(creator.get("service") or "")
            cid = str(creator.get("id") or "")
            post_id = str(getattr(media_item, "post_id", "") or "")
            fname = str(getattr(media_item, "filename", "") or "")
            url = str(getattr(media_item, "url", "") or "")
            raw = f"{service}|{cid}|{post_id}|{fname}|{url}".encode("utf-8", "ignore")
            return int(zlib.crc32(raw) & 0xFFFFFFFF)
        except Exception:
            return int(zlib.crc32(repr(media_item).encode("utf-8", "ignore")) & 0xFFFFFFFF)

    def _creator_popularity_score(self, creator: dict) -> float:
        """Best-effort popularity score from whatever numeric fields exist."""
        score = 0.0
        weights = {
            "favorited": 5.0,
            "favorites": 5.0,
            "subscribers": 4.0,
            "subscriber_count": 4.0,
            "posts": 2.0,
            "post_count": 2.0,
            "posts_count": 2.0,
            "indexed": 1.0,
        }
        for k, w in weights.items():
            v = creator.get(k)
            if isinstance(v, (int, float)):
                score += float(v) * w
            elif isinstance(v, str) and v.isdigit():
                score += float(int(v)) * w
        if score <= 0.0:
            try:
                score = float(int(creator.get("id") or 0))
            except Exception:
                score = 0.0
        return score

    async def _send_random_previews_from_coomer(self, bot, model: str, qty: int, dest_chat_ids: list[int]) -> Tuple[int, int]:
        """Send previews using Coomer API as the source (photos + videos)."""
        sent_count = 0
        error_count = 0

        uploader = self.uploader
        if uploader is None:
            try:
                uploader = TelegramUploader(bot)
            except Exception as e:
                logger.warning("Uploader init failed in coomer autopreviews: %s", e)
                return 0, 1

        model_q = (model or "random").strip()
        model_l = model_q.lower()

        def _already_sent(dest_id: int, key: int) -> bool:
            try:
                if hasattr(self.preview_db, "has_sent") and callable(getattr(self.preview_db, "has_sent")):
                    return bool(self.preview_db.has_sent(dest_chat_id=int(dest_id), model="coomer", vip_chat_id=0, vip_message_id=int(key)))
            except Exception:
                return False
            return False

        async with MediaFetcher() as fetcher:
            creators: list[dict] = []
            if model_l not in ("random", "all", "*", "popular", "top"):
                try:
                    cr = await fetcher.find_creator(model_q)
                    if cr:
                        creators = [cr]
                except Exception as e:
                    logger.warning("Coomer autopreviews: find_creator failed for '%s': %s", model_q, e)

            if not creators:
                try:
                    all_creators = await fetcher._get_creators_list()
                except Exception as e:
                    logger.warning("Coomer autopreviews: creators list fetch failed: %s", e)
                    all_creators = []

                if not all_creators:
                    return 0, 0

                try:
                    all_creators = sorted(all_creators, key=self._creator_popularity_score, reverse=True)
                except Exception:
                    pass

                top_n = all_creators[:300] if len(all_creators) > 300 else all_creators
                try:
                    k = min(12, len(top_n))
                    creators = random.sample(top_n, k=k) if k > 0 else []
                except Exception:
                    creators = top_n[:12]

            if not creators:
                return 0, 0

            pool: list[tuple[dict, MediaItem]] = []
            for cr in creators:
                try:
                    items = await fetcher.fetch_posts_paged(cr, offset=0)
                except Exception as e:
                    logger.warning("Coomer autopreviews: fetch_posts_paged failed (%s): %s", cr.get("name") or cr.get("id"), e)
                    continue
                for it in items or []:
                    if not getattr(it, "url", None):
                        continue
                    pool.append((cr, it))
                    if len(pool) >= max(10, int(qty) * 3):
                        break
                if len(pool) >= max(10, int(qty) * 3):
                    break

            if not pool:
                return 0, 0

            for dest in dest_chat_ids:
                dest_id = int(dest)
                sent_for_dest = 0
                random.shuffle(pool)

                for cr, media in pool:
                    if sent_for_dest >= int(qty):
                        break

                    key = self._coomer_item_key(cr, media)
                    if _already_sent(dest_id, key):
                        continue

                    creator_name = str(cr.get("name") or cr.get("username") or cr.get("id") or "modelo")

                    try:
                        await fetcher.download_media(media)
                    except TooLargeMedia as tl:
                        # Skip oversized files entirely (no link fallback).
                        try:
                            logger.info(
                                "Coomer autopreviews: skipping oversized media (%.2f MB > %.2f MB) url=%s",
                                (getattr(tl, "size_bytes", 0) or 0) / (1024 * 1024),
                                (getattr(tl, "max_bytes", 0) or 0) / (1024 * 1024),
                                getattr(tl, "url", None) or getattr(media, "url", None),
                            )
                        except Exception:
                            pass
                        continue
                    except Exception as e:
                        logger.warning("Coomer autopreviews: download_media failed dest=%s: %s", dest_id, e)
                        error_count += 1
                        continue

                    to_send = media
                    caption = f"👀 Preview • {creator_name}"
                    try:
                        if getattr(media, "media_type", "photo") == "photo":
                            prev = None
                            try:
                                prev = PreviewGenerator.create_preview(media)
                            except Exception:
                                prev = None
                            if prev:
                                to_send = prev
                        else:
                            caption = f"🎬 Preview (vídeo) • {creator_name}"
                    except Exception:
                        pass

                    try:
                        msg_id = await uploader._upload_single(channel_id=dest_id, media_item=to_send, caption=caption)
                        if msg_id:
                            sent_for_dest += 1
                            sent_count += 1
                            try:
                                self.preview_db.mark_sent(dest_chat_id=dest_id, model="coomer", vip_chat_id=0, vip_message_id=int(key), file_unique_id=str(getattr(media, "filename", "") or ""))
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning("Coomer autopreviews: upload failed dest=%s: %s", dest_id, e)
                        error_count += 1
                    finally:
                        for p in {getattr(media, "local_path", None), getattr(to_send, "local_path", None)}:
                            if p and isinstance(p, str) and os.path.exists(p):
                                try:
                                    os.remove(p)
                                except Exception:
                                    pass

        return sent_count, error_count

    async def _send_random_previews(self, bot, model: str, qty: int, dest_chat_ids: list[int]) -> Tuple[int, int]:
        """Send random, unsent previews to multiple destinations.

        Returns (sent_count, error_count).
        """
        sent_count = 0
        error_count = 0
        # Prefer Coomer API as the source (photos + videos). If it sends something, stop here.
        try:
            sc, ec = await self._send_random_previews_from_coomer(bot=bot, model=model, qty=qty, dest_chat_ids=dest_chat_ids)
            sent_count += int(sc or 0)
            error_count += int(ec or 0)
            if sent_count > 0:
                return sent_count, error_count
        except Exception as e:
            logger.warning("Coomer autopreviews primary source failed (falling back to VIP forwarding): %s", e)

        vip_chat_id = getattr(config, "VIP_CHANNEL_ID", None)
        if not vip_chat_id:
            return 0, 0

        for dest in dest_chat_ids:
            try:
                items = self.preview_db.get_random_unsent(model=model, dest_chat_id=int(dest), limit=int(qty))
            except Exception as e:
                logger.warning(f"Random previews get_random_unsent failed dest={dest}: {e}")
                error_count += 1
                continue

            if not items:
                continue

            for it in items:
                try:
                    await bot.copy_message(
                        chat_id=int(dest),
                        from_chat_id=int(it["vip_chat_id"]),
                        message_id=int(it["vip_message_id"]),
                    )
                    self.preview_db.mark_sent(
                        dest_chat_id=int(dest),
                        model=str(it.get("model", model)),
                        vip_chat_id=int(it["vip_chat_id"]),
                        vip_message_id=int(it["vip_message_id"]),
                        file_unique_id=it.get("file_unique_id"),
                    )
                    sent_count += 1
                except Exception as e:
                    # Robustness: Chat not found / message not found / entity parsing, etc.
                    msg = str(e).lower()
                    if "chat not found" in msg or "not found" in msg and "chat" in msg:
                        logger.warning(f"Random previews: chat not found dest={dest}: {e}")
                        error_count += 1
                        break
                    if "message to copy not found" in msg:
                        logger.warning(
                            f"Random previews: message to copy not found vip={it.get('vip_chat_id')}:{it.get('vip_message_id')}: {e}"
                        )
                        error_count += 1
                        continue
                    logger.warning(f"Random previews copy error dest={dest}: {e}")
                    error_count += 1
                    continue

        return sent_count, error_count

    # ---------- VIP channel auto-index ----------
    async def on_vip_channel_post(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Silently index media posts that appear in the VIP channel."""
        try:
            msg = update.effective_message
            chat = update.effective_chat
            if not msg or not chat:
                return
            vip_chat_id = getattr(config, "VIP_CHANNEL_ID", None)
            if not vip_chat_id or int(chat.id) != int(vip_chat_id):
                return

            media_type = "unknown"
            file_unique_id = None
            if msg.photo:
                media_type = "photo"
                file_unique_id = msg.photo[-1].file_unique_id
            elif msg.video:
                media_type = "video"
                file_unique_id = msg.video.file_unique_id
            elif msg.document:
                media_type = "document"
                file_unique_id = msg.document.file_unique_id

            caption = msg.caption or msg.text or ""
            model = extract_model_from_caption(caption)

            ok = self.preview_db.add_vip_message(
                vip_chat_id=int(chat.id),
                vip_message_id=int(msg.message_id),
                model=str(model),
                media_type=str(media_type),
                file_unique_id=file_unique_id,
                caption=caption or None,
            )
            if ok:
                logger.info(
                    f"Indexed VIP message vip_chat_id={chat.id} msg_id={msg.message_id} model={model} media_type={media_type}"
                )
        except Exception as e:
            logger.warning(f"VIP auto-index error (ignored): {e}")

    async def pv_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            await update.message.reply_text("Cancelado.")
        except Exception:
            pass
        return ConversationHandler.END

    async def cmd_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /search command"""
        if not await self.check_authorization(update):
            return
        
        user_id = update.effective_user.id
        lang = user_manager.get_language(user_id)
        
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(get_text("search_usage", lang))
            return
        
        model_name = " ".join(context.args)
        user_manager.increment_searches(user_id)
        
        # Ask user to select source
        
        keyboard = [
            [InlineKeyboardButton("🔵 Coomer.st", callback_data=f"source:coomer:{model_name}")],
            [InlineKeyboardButton("🟠 Picazor.com", callback_data=f"source:picazor:{model_name}")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="cancel_search")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        status_msg = await update.message.reply_text(
            f"🔍 Buscar: {model_name}\n\nSelecione a fonte de mídias:",
            parse_mode=None,
            reply_markup=reply_markup
        )

    async def _show_page(self, update: Update, user_id: int, page_idx: int, status_msg=None):
        """Show a page of results"""
        cache = self.search_cache.get(user_id)
        if not cache:
            return

        items = cache['pages'].get(page_idx, [])
        count = len(items)
        model_name = cache['model_name']
        total_posts = cache.get('total_posts', 0)
        total_uploaded = cache.get('total_uploaded', 0)
        
        # Calculate estimated total pages
        estimated_pages = (total_posts // 50) + 1 if total_posts > 0 else "?"
        
        text = f"✅ {model_name}\n\n"
        text += f"📄 Página: {page_idx + 1}/{estimated_pages}\n"
        text += f"📦 Mídias neste slot: {count}\n"
        text += f"📊 Total de posts: {total_posts}\n"
        text += f"✅ Já enviados: {total_uploaded}\n\n"
        text += "Escolha uma opção abaixo:"

        buttons = []
        
        if count > 0:
            buttons.append([
                InlineKeyboardButton(f"📥 Download Página {page_idx + 1}", callback_data=f"dl_{page_idx}")
            ])
            buttons.append([
                InlineKeyboardButton("🚀 Download TUDO (Automático)", callback_data=f"dlall_{page_idx}")
            ])
        
        nav_row = []
        if page_idx > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Anterior", callback_data=f"page_{page_idx-1}"))
        nav_row.append(InlineKeyboardButton("➡️ Próxima", callback_data=f"page_{page_idx+1}"))
        buttons.append(nav_row)
        
        buttons.append([InlineKeyboardButton("🛑 Parar e Enviar Previews", callback_data="stop_send_previews")])
            
        reply_markup = InlineKeyboardMarkup(buttons)
        
        if status_msg:
            edited = await self._safe_edit_message_obj(status_msg, text, reply_markup=reply_markup, parse_mode=None)
            if edited is None:
                # Fallback: send a new message if edit is rate-limited or fails
                chat_id = status_msg.chat_id if hasattr(status_msg, 'chat_id') else update.effective_chat.id
                await self._safe_send_message(chat_id, text, reply_markup=reply_markup, parse_mode=None)
        else:
            edited = await self._safe_edit_message_text(update.callback_query, text, reply_markup=reply_markup, parse_mode=None)
            if edited is None and update.effective_chat:
                await self._safe_send_message(update.effective_chat.id, text, reply_markup=reply_markup, parse_mode=None)



    async def _safe_cb_answer(self, query, text: str | None = None, show_alert: bool = False) -> bool:
        # answerCallbackQuery is lightweight but still rate-limited. We throttle softly.
        chat_id = None
        try:
            if query and getattr(query, 'message', None) and getattr(query.message, 'chat', None):
                chat_id = query.message.chat.id
        except Exception:
            chat_id = None

        try:
            if chat_id is not None:
                await self._tg_soft_throttle(chat_id)
            await query.answer(text=text, show_alert=show_alert)
        except RetryAfter as e:
            retry_after = float(getattr(e, 'retry_after', 0) or 0)
            self._tg_register_cooldown(retry_after, chat_id=chat_id)
            logger.warning('Flood control on answerCallbackQuery: retry_after=%s', getattr(e, 'retry_after', None))
            return False
        except Exception:
            return False
        return True

    def _tg_now(self) -> float:
        return time.monotonic()

    def _tg_register_cooldown(self, retry_after: float, chat_id: int | None = None) -> None:
        """Record Telegram cooldown (global and optionally per-chat) to avoid spamming 429s."""
        try:
            retry_after = float(retry_after or 0)
        except Exception:
            retry_after = 0.0
        if retry_after <= 0:
            return
        now = self._tg_now()
        # Global cooldown helps when Telegram rate-limits the bot globally.
        self._tg_cooldown_until = max(float(self._tg_cooldown_until or 0.0), now + retry_after)
        if chat_id is not None:
            try:
                chat_id_int = int(chat_id)
                prev = float(self._tg_cooldown_until_per_chat.get(chat_id_int, 0.0) or 0.0)
                self._tg_cooldown_until_per_chat[chat_id_int] = max(prev, now + retry_after)
            except Exception:
                pass

    def _tg_in_cooldown(self, chat_id: int | None = None) -> bool:
        now = self._tg_now()
        try:
            if now < float(self._tg_cooldown_until or 0.0):
                return True
        except Exception:
            pass
        if chat_id is None:
            return False
        try:
            chat_id_int = int(chat_id)
            return now < float(self._tg_cooldown_until_per_chat.get(chat_id_int, 0.0) or 0.0)
        except Exception:
            return False

    async def _tg_soft_throttle(self, chat_id: int) -> None:
        """Soft per-chat throttle to reduce flood risk without hard-blocking."""
        try:
            now = self._tg_now()
            last = float(self._tg_last_call_per_chat.get(chat_id, 0.0) or 0.0)
            wait = self._tg_min_interval_seconds - (now - last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._tg_last_call_per_chat[chat_id] = self._tg_now()
        except Exception:
            return

    async def _safe_edit_message_text(self, query, text: str, **kwargs):
        kwargs.setdefault('parse_mode', None)
        try:
            # If Telegram told us to cool down, skip edits to avoid repeated 429s.
            chat_id = None
            try:
                if query and getattr(query, 'message', None) and getattr(query.message, 'chat', None):
                    chat_id = query.message.chat.id
            except Exception:
                chat_id = None
            if chat_id is not None:
                if self._tg_in_cooldown(chat_id):
                    return None
                await self._tg_soft_throttle(chat_id)
            return await query.edit_message_text(text, **kwargs)
        except RetryAfter as e:
            retry_after = float(getattr(e, 'retry_after', 0) or 0)
            self._tg_register_cooldown(retry_after, chat_id=chat_id)
            logger.warning('Flood control on edit_message_text: retry_after=%s', getattr(e, 'retry_after', None))
            # Avoid extra API calls when cooldown is huge.
            if retry_after and retry_after <= 15:
                await self._safe_cb_answer(query, text='⏳ Limite do Telegram atingido. Tente novamente em alguns segundos.', show_alert=True)
            return None
        except BadRequest as e:
            msg = str(e)
            if 'Message is not modified' in msg:
                return None
            logger.warning('BadRequest on edit_message_text: %s', msg)
            return None
        except Forbidden as e:
            logger.warning('Forbidden on edit_message_text: %s', e)
            return None
        except TelegramError as e:
            logger.warning('TelegramError on edit_message_text: %s', e)
            return None


    async def _safe_edit_message_obj(self, message, text: str, **kwargs):
        """Safely edit a Message object (message.edit_text) handling Telegram rate limits."""
        kwargs.setdefault('parse_mode', None)
        try:
            chat_id = None
            try:
                if message and getattr(message, 'chat', None):
                    chat_id = message.chat.id
            except Exception:
                chat_id = None
            if chat_id is not None:
                if self._tg_in_cooldown(chat_id):
                    return None
                await self._tg_soft_throttle(chat_id)
            return await message.edit_text(text, **kwargs)
        except RetryAfter as e:
            retry_after = float(getattr(e, 'retry_after', 0) or 0)
            self._tg_register_cooldown(retry_after, chat_id=chat_id)
            logger.warning('Flood control on message.edit_text: retry_after=%s', getattr(e, 'retry_after', None))
            return None
        except BadRequest as e:
            msg = str(e)
            if 'Message is not modified' in msg:
                return None
            logger.warning('BadRequest on message.edit_text: %s', msg)
            return None
        except Forbidden as e:
            logger.warning('Forbidden on message.edit_text: %s', e)
            return None
        except TelegramError as e:
            logger.warning('TelegramError on message.edit_text: %s', e)
            return None

    async def _safe_send_message(self, chat_id: int, text: str, **kwargs):
        kwargs.setdefault('parse_mode', None)
        try:
            if self._tg_in_cooldown(chat_id):
                return None
            await self._tg_soft_throttle(chat_id)
            return await self.app.bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except RetryAfter as e:
            retry_after = float(getattr(e, 'retry_after', 0) or 0)
            self._tg_register_cooldown(retry_after, chat_id=chat_id)
            logger.warning('Flood control on send_message chat_id=%s retry_after=%s', chat_id, getattr(e, 'retry_after', None))
            return None
        except Forbidden as e:
            logger.warning('Forbidden on send_message chat_id=%s: %s', chat_id, e)
            return None
        except TelegramError as e:
            logger.warning('TelegramError on send_message chat_id=%s: %s', chat_id, e)
            return None
        except (TimedOut, NetworkError) as e:
            logger.warning('Network error on send_message chat_id=%s: %s', chat_id, e)
            return None

    async def on_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button clicks"""
        query = update.callback_query
        await self._safe_cb_answer(query)
        
        user_id = update.effective_user.id
        data = query.data

        # Admin menu (buttons)
        if data == "admin:menu":
            if user_id != config.ADMIN_ID:
                return
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📢 Broadcast", callback_data="admin:broadcast"),
                    InlineKeyboardButton("🖼️ Previews", callback_data="admin:previews"),
                ],
                [
                    InlineKeyboardButton("🤖 Modo automático", callback_data="admin:auto"),
                ],
            ])
            text_msg = "🛠️ Painel Admin\n\nEscolha uma opção:"
            try:
                await query.edit_message_text(text_msg, parse_mode=None, reply_markup=keyboard)
            except Exception:
                await self._safe_send_message(query.message.chat_id, text_msg, reply_markup=keyboard)
            return

        # Admin: total automatic mode (autopreviews + VIP feed)
        if data == "admin:auto" or data == "admin:auto:toggle":
            if user_id != config.ADMIN_ID:
                return

            # Toggle if requested
            try:
                ap_cfg = self.preview_db.get_auto_config()
                vf_cfg = self.preview_db.get_vip_feed_config()
            except Exception as e:
                logger.error("Admin auto mode: failed to read config: %s", e)
                return

            # Overall enabled if BOTH modules enabled
            overall_on = bool(int(ap_cfg.get("enabled", 0)) and int(vf_cfg.get("enabled", 0)))

            if data == "admin:auto:toggle":
                new_enabled = 0 if overall_on else 1
                try:
                    # Keep existing autopreviews settings; only flip enabled
                    self.preview_db.set_auto_config(
                        enabled=new_enabled,
                        interval_minutes=int(ap_cfg.get("interval_minutes", 1440)),
                        qty=int(ap_cfg.get("qty", 5)),
                        model=str(ap_cfg.get("model", "all")),
                        send_to_free=int(ap_cfg.get("send_to_free", 1)),
                        send_to_private=int(ap_cfg.get("send_to_private", 0)),
                        admin_chat_id=ap_cfg.get("admin_chat_id"),
                        free_mode=str(ap_cfg.get("free_mode", "all")),
                        free_chat_id=ap_cfg.get("free_chat_id"),
                    )
                except Exception as e:
                    logger.error("Admin auto mode: failed to update autopreviews: %s", e)

                try:
                    self.preview_db.set_vip_feed_config(
                        enabled=new_enabled,
                        interval_minutes=int(vf_cfg.get("interval_minutes", 60)),
                        page_items=int(vf_cfg.get("page_items", 10)),
                        top_n=int(vf_cfg.get("top_n", 100)),
                    )
                except Exception as e:
                    logger.error("Admin auto mode: failed to update vip feed: %s", e)

                # Refresh
                try:
                    ap_cfg = self.preview_db.get_auto_config()
                    vf_cfg = self.preview_db.get_vip_feed_config()
                except Exception:
                    pass
                overall_on = bool(int(ap_cfg.get("enabled", 0)) and int(vf_cfg.get("enabled", 0)))
                try:
                    if new_enabled:
                        # Ensure background loops are running
                        self._start_auto_previews_task()
                        self._start_vip_feed_task()
                    else:
                        self._stop_auto_previews_task()
                        self._stop_vip_feed_task()
                except Exception as e:
                    logger.warning("Admin auto mode: task toggle failed (ignored): %s", e)

            # Render status panel
            ap_enabled = int(ap_cfg.get("enabled", 0))
            vf_enabled = int(vf_cfg.get("enabled", 0))

            ap_dest = []
            if int(ap_cfg.get("send_to_free", 0)):
                ap_dest.append("free")
            if int(ap_cfg.get("send_to_private", 0)):
                ap_dest.append("private")
            ap_dest_str = "+".join(ap_dest) if ap_dest else "none"

            status_lines = [
                "🤖 Modo automático",
                "",
                f"• Total: {'ON ✅' if overall_on else 'OFF ❌'}",
                f"• AutoPreviews: {'ON' if ap_enabled else 'OFF'} | interval={ap_cfg.get('interval_minutes')}m qty={ap_cfg.get('qty')} model={ap_cfg.get('model')} dest={ap_dest_str}",
                f"• VIP Feed: {'ON' if vf_enabled else 'OFF'} | interval={vf_cfg.get('interval_minutes')}m itens={vf_cfg.get('page_items')} top={vf_cfg.get('top_n')}",
            ]
            text_msg = "\n".join([str(x) for x in status_lines])

            toggle_label = "🟢 Desligar (Total ON)" if overall_on else "🔴 Ligar (Total OFF)"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(toggle_label, callback_data="admin:auto:toggle")],
                [InlineKeyboardButton("⬅️ Voltar", callback_data="admin:menu")],
            ])

            try:
                await query.edit_message_text(text_msg, parse_mode=None, reply_markup=keyboard)
            except Exception:
                # If edit fails (e.g., message not editable), send new
                await self._safe_send_message(query.message.chat_id, text_msg, reply_markup=keyboard)
            return

        if data.startswith("source:"):
            # Handle source selection
            parts = data.split(":")
            if len(parts) >= 3:
                source = parts[1]  # 'coomer' or 'picazor'
                model_name = ":".join(parts[2:])  # Rejoin in case model name has ':'
                
                await self._safe_edit_message_text(query, 
                    f"🔍 Buscando {model_name} em {source.capitalize()}...",
                    parse_mode=None
                )
                
                try:
                    from source_handler import SourceHandler
                    
                    # Find creator in selected source
                    creator = await SourceHandler.search_source(source, model_name)
                    
                    if not creator:
                        # Show similar matches if available
                        matches = await SourceHandler.find_all_matching(source, model_name)
                        if matches:
                            
                            text = f"🔍 Encontramos {len(matches)} modelo(s) similar(es) a '{model_name}' em {source.capitalize()}:\n\n"
                            text += "Selecione a modelo correta:"
                            
                            keyboard = []
                            for m in matches[:8]:  # Max 8 options
                                creator_name = m.get('name')
                                button_text = f"{creator_name}"
                                callback_data = f"select_model_src:{source}:{creator_name}"
                                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                            
                            keyboard.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel_search")])
                            reply_markup = InlineKeyboardMarkup(keyboard)
                            
                            await self._safe_edit_message_text(query, text, parse_mode=None, reply_markup=reply_markup)
                        else:
                            await self._safe_edit_message_text(query, f"❌ Nenhuma modelo encontrada para '{model_name}' em {source.capitalize()}.")
                        return
                    
                    # Creator found, fetch first page
                    creator_name = creator.get('name')
                    
                    await self._safe_edit_message_text(query, 
                        f"✅ Encontrado: {creator_name} ({source.capitalize()})\n🔄 Carregando mídias...",
                        parse_mode=None
                    )
                    
                    # Fetch first page
                    first_page_items = await SourceHandler.fetch_posts(source, creator, offset=0)
                    
                    if not first_page_items:
                        await self._safe_edit_message_text(query, 
                            f"❌ Nenhuma mídia encontrada para **{escape_markdown(creator_name)}**",
                            parse_mode=None
                        )
                        return
                    
                    # Store in cache
                    self.search_cache[user_id] = {
                        'model_name': creator_name,
                        'creator': creator,
                        'source': source,
                        'pages': {0: first_page_items},
                        'current_page': 0,
                        'total_sent': 0,
                        'total_uploaded': 0,
                        'uploaded_items': [],
                        'sent_media_ids': set(),
                        'abort_flag': False  # Flag to abort downloads
                    }
                    
                    await self._show_page(update, user_id, 0)
                
                except RetryAfter as e:
                    # Don't abort search flow on Telegram flood control; results may already be cached.
                    logger.warning("Flood control during search flow source=%s retry_after=%s", source, getattr(e, "retry_after", None))
                    await self._safe_cb_answer(query, text="⏳ Limite do Telegram atingido. Tente novamente em alguns segundos.", show_alert=True)
                except Exception as e:
                    logger.error(f"Error searching {source}: {e}")
                    await self._safe_edit_message_text(query, f"❌ Erro: {e}")
            return
        
        if data.startswith("select_model_src:"):
            # Handle model selection with source
            parts = data.split(":")
            if len(parts) >= 3:
                source = parts[1]
                creator_name = ":".join(parts[2:])
                
                await self._safe_edit_message_text(query, 
                    f"✅ Modelo selecionada: **{escape_markdown(creator_name)}**\n🔄 Carregando mídias...",
                    parse_mode=None
                )
                
                try:
                    from source_handler import SourceHandler
                    
                    creator = await SourceHandler.search_source(source, creator_name)
                    
                    if not creator:
                        await query.message.reply_text("❌ Erro ao carregar modelo.")
                        return
                    
                    # Get first page
                    first_page_items = await SourceHandler.fetch_posts(source, creator, offset=0)
                    
                    if not first_page_items:
                        await query.message.reply_text(
                            f"❌ Nenhuma mídia encontrada para **{escape_markdown(creator_name)}**",
                            parse_mode=None
                        )
                        return
                    
                    # Store in cache
                    self.search_cache[user_id] = {
                        'model_name': creator_name,
                        'creator': creator,
                        'source': source,
                        'pages': {0: first_page_items},
                        'current_page': 0,
                        'total_sent': 0,
                        'total_uploaded': 0,
                        'uploaded_items': [],
                        'sent_media_ids': set(),
                        'abort_flag': False
                    }
                    
                    await self._show_page(update, user_id, 0)
                except Exception as e:
                    logger.error(f"Error loading selected model: {e}")
                    await query.message.reply_text(f"❌ Erro: {e}")
            return
        
        if data.startswith("select_model:"):
            # Handle model selection from search results
            parts = data.split(":")
            if len(parts) >= 3:
                creator_name = parts[1]
                service = parts[2]
                
                await self._safe_edit_message_text(query, 
                    f"✅ Modelo selecionada: **{creator_name}** ({service})\n🔄 Carregando mídias...",
                    parse_mode=None
                )
                
                # Simulate a search with the selected creator
                try:
                    async with MediaFetcher() as fetcher:
                        creator = await fetcher.find_creator(creator_name)
                        
                        if not creator:
                            await query.message.reply_text("❌ Erro ao carregar modelo.")
                            return
                        
                        # Get first page
                        first_page_items = await fetcher.fetch_posts_paged(creator, offset=0)
                        
                        if not first_page_items:
                            await query.message.reply_text(
                                f"❌ Nenhuma mídia encontrada para **{creator_name}**",
                                parse_mode=None
                            )
                            return
                        
                        # Store in cache
                        self.search_cache[user_id] = {
                            'model_name': creator_name,
                            'creator': creator,
                            'pages': {0: first_page_items},
                            'current_page': 0,
                            'total_sent': 0,
                            'sent_media_ids': set()
                        }
                        
                        await self._show_page(update, user_id, 0)
                except Exception as e:
                    logger.error(f"Error loading selected model: {e}")
                    await query.message.reply_text(f"❌ Erro: {e}")
            return
        
        if data == "cancel_search":
            await self._safe_edit_message_text(query, "❌ Busca cancelada.")
            return
        
        if data == "abort_download":
            # Set abort flag
            cache = self.search_cache.get(user_id)
            if cache:
                cache['abort_flag'] = True
                await query.answer("⛔ Abortando download...", show_alert=True)
            return
        
        if data == "stop_send_previews":
            await self._finalize_and_send_previews(update, user_id)
            return
        
        if data.startswith("page_"):
            page_idx = int(data.split("_")[1])
            cache = self.search_cache.get(user_id)
            if not cache:
                await self._safe_edit_message_text(query, "❌ Sessão expirada. Use /search novamente.")
                return

            # If page not in cache, fetch it
            if page_idx not in cache['pages']:
                await self._safe_edit_message_text(query, "🔄 Carregando página...")
                
                try:
                    async with MediaFetcher() as fetcher:
                        new_offset = page_idx * 50
                        new_items = await fetcher.fetch_posts_paged(cache['creator'], offset=new_offset)
                        
                        if not new_items:
                            await query.message.reply_text("❌ Não há mais mídias disponíveis.")
                            await self._show_page(update, user_id, max(0, page_idx - 1))
                            return
                        
                        cache['pages'][page_idx] = new_items
                        logger.info(f"Page {page_idx} loaded with {len(new_items)} items")
                except Exception as e:
                    logger.error(f"Error fetching page: {e}")
                    await self._safe_edit_message_text(query, f"❌ Erro ao carregar página: {e}")
                    return
            
            await self._show_page(update, user_id, page_idx)
            
        elif data.startswith("dl_"):
            page_idx = int(data.split("_")[1])
            await self._process_download_slot(update, user_id, page_idx, auto_continue=False)
            
        elif data.startswith("dlall_"):
            page_idx = int(data.split("_")[1])
            await self._process_download_slot(update, user_id, page_idx, auto_continue=True)

    async def _process_download_slot(self, update: Update, user_id: int, page_idx: int, auto_continue: bool = False):
        """Process download and upload for a specific page"""
        cache = self.search_cache.get(user_id)
        
        if not cache:
            await update.callback_query.edit_message_text("❌ Sessão expirada. Use /search novamente.")
            return

        slot_items = cache['pages'].get(page_idx, [])
        if not slot_items:
            await update.callback_query.edit_message_text("❌ Nenhuma mídia neste slot.")
            return
            
        model_name = cache['model_name']
        status_msg = update.callback_query.message
        
        # Avoid heavy Markdown and excessive edits (can trigger Telegram flood control).
        await self._safe_edit_message_obj(
            status_msg,
            f"⏳ Iniciando Slot {page_idx + 1}\n"
            f"📦 Mídias: {len(slot_items)}\n"
            f"🔄 Conectando aos servidores...",
            parse_mode=None,
        )
        
        vip_count = 0
        failed_count = 0
        total_in_slot = len(slot_items)
        
        last_ui_update = 0.0

        async with MediaFetcher() as fetcher:
            for i, item in enumerate(slot_items):
                # Check abort flag
                if cache.get('abort_flag', False):
                    await self._safe_edit_message_obj(
                        status_msg,
                        f"⛔ Download abortado pelo usuário\n\n"
                        f"✅ Enviados: {vip_count}\n"
                        f"❌ Falhas: {failed_count}\n\n"
                        f"Use /search para iniciar nova busca.",
                        parse_mode=None,
                    )
                    return
                
                try:
                    # Update progress
                    progress_pct = ((i + 1) / total_in_slot) * 100
                    progress_bar = "█" * int(progress_pct / 10) + "░" * (10 - int(progress_pct / 10))
                    
                    progress_text = f"📥 **Processando Slot {page_idx + 1}**\n\n"
                    progress_text += f"[{progress_bar}] {progress_pct:.0f}%\n"
                    progress_text += f"📊 {i+1}/{total_in_slot}\n"
                    progress_text += f"✅ Enviados: {vip_count}\n"
                    progress_text += f"❌ Falhas: {failed_count}\n\n"
                    progress_text += f"📦 `{item.filename[:35]}...`"
                    
                    # Add abort button
                    abort_button = InlineKeyboardMarkup([
                        [InlineKeyboardButton("⛔ Abortar Download", callback_data="abort_download")]
                    ])
                    
                    # Throttle UI edits to reduce flood risk.
                    now = self._tg_now()
                    should_update = (i == 0) or (i == total_in_slot - 1) or (now - last_ui_update >= 1.2)
                    if should_update:
                        last_ui_update = now
                        await self._safe_edit_message_obj(
                            status_msg,
                            progress_text.replace('**', ''),  # strip Markdown markers; parse_mode=None
                            parse_mode=None,
                            reply_markup=abort_button,
                        )
                    
                    # Download (with size guard).
                    try:
                        success = await fetcher.download_media(item)
                    except TooLargeMedia as tl:
                        # Skip oversized files entirely (no link fallback).
                        try:
                            logger.info(
                                "VIP index: skipping oversized media (%.2f MB > %.2f MB) url=%s",
                                (getattr(tl, "size_bytes", 0) or 0) / (1024 * 1024),
                                (getattr(tl, "max_bytes", 0) or 0) / (1024 * 1024),
                                getattr(tl, "url", None),
                            )
                        except Exception:
                            pass
                        failed_count += 1
                        continue

                    if not success:
                        logger.warning(f"Failed to download: {item.url}")
                        failed_count += 1
                        continue
                    
                    # Upload to VIP channel
                    uploaded = await self.uploader.upload_and_cleanup(
                        item,
                        config.VIP_CHANNEL_ID,
                        caption=f"🔥 {model_name}",
                        model_name=model_name,
                    )
                    
                    if uploaded:
                        vip_count += 1
                        cache['total_uploaded'] += 1
                        # Store item info for previews later
                        cache['uploaded_items'].append({
                            'url': item.url,
                            'type': item.media_type,
                            'filename': item.filename
                        })
                    else:
                        failed_count += 1

                    # Delay to avoid rate limits (2 seconds between uploads)
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing item {i}: {e}")
                    failed_count += 1

        # Slot complete
        final_text = f"✅ **Slot {page_idx + 1} Concluído!**\n\n"
        final_text += f"📊 **Resultados:**\n"
        final_text += f"✅ Enviados: {vip_count}\n"
        final_text += f"❌ Falhas: {failed_count}\n"
        final_text += f"📈 Total enviados: {cache['total_uploaded']}\n"
        
        await self._safe_edit_message_obj(status_msg, final_text.replace('**', ''), parse_mode=None)
        await asyncio.sleep(2)
        
        # Auto-continue to next page if enabled
        if auto_continue:
            next_page = page_idx + 1
            
            # Fetch next page
            async with MediaFetcher() as fetcher:
                new_offset = next_page * 50
                new_items = await fetcher.fetch_posts_paged(cache['creator'], offset=new_offset)
                
                if new_items:
                    cache['pages'][next_page] = new_items
                    await self._safe_edit_message_obj(
                        status_msg,
                        f"🔄 Continuando automaticamente...\n"
                        f"Próxima página: {next_page + 1}\n"
                        f"Mídias: {len(new_items)}",
                        parse_mode=None,
                    )
                    await asyncio.sleep(3)  # Delay between pages
                    await self._process_download_slot(update, user_id, next_page, auto_continue=True)
                    return
                else:
                    # No more pages, finalize
                    await self._finalize_and_send_previews(update, user_id)
                    return
        
        # Show page again
        await self._show_page(update, user_id, page_idx, status_msg)

    async def _finalize_and_send_previews(self, update: Update, user_id: int):
        """Finalize the process and send previews to FREE channels"""
        cache = self.search_cache.get(user_id)
        
        if not cache:
            await update.callback_query.edit_message_text("❌ Sessão expirada.")
            return
        
        model_name = cache['model_name']
        total_uploaded = cache.get('total_uploaded', 0)
        uploaded_items = cache.get('uploaded_items', [])
        status_msg = update.callback_query.message
        
        await status_msg.edit_text(
            f"🏁 **Finalizando...**\n\n"
            f"📊 Total enviados para VIP: {total_uploaded}\n"
            f"🖼️ Encaminhando previews aleatórias do VIP para canais FREE...",
            parse_mode=None
        )
        
        # Send previews by forwarding from VIP
        preview_count = 0
        
        if total_uploaded > 0:
            try:
                await self.uploader.send_previews_from_vip(model_name)
                preview_count = config.get_value("PREVIEW_LIMIT", 3)
                
                await status_msg.edit_text(
                    f"✅ **Previews enviados com sucesso!**\n\n"
                    f"📊 {preview_count} mídias aleatórias encaminhadas para os canais FREE.",
                    parse_mode=None
                )
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error forwarding previews: {e}")
                await status_msg.edit_text(
                    f"⚠️ Erro ao enviar previews: {e}",
                    parse_mode=None
                )
                await asyncio.sleep(2)
        
        # Final summary
        final_text = f"🎉 **Processo Concluído!**\n\n"
        final_text += f"👤 Modelo: **{model_name}**\n"
        final_text += f"✅ Mídias enviadas para VIP: {total_uploaded}\n"
        final_text += f"🖼️ Previews enviados para FREE: {preview_count}\n\n"
        final_text += "Use /search para buscar outra modelo."
        
        await status_msg.edit_text(final_text, parse_mode=None)
        
        # Clear cache
        if user_id in self.search_cache:
            del self.search_cache[user_id]
    

    async def cmd_ref(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Generate a personal referral link.

        In public referral mode, this command is available to everyone.
        In private mode, only whitelisted users can use it.
        """
        if not await self.check_authorization(update, public_ok=True):
            return

        user = update.effective_user
        user_manager.get_user(user.id)
        lang = user_manager.get_language(user.id)

        bot_username = getattr(context.bot, 'username', None) or ''
        link = f"https://t.me/{bot_username}?start=ref_{user.id}" if bot_username else f"/start ref_{user.id}"
        count = self.referrals.get_referral_count(user.id)

        goals = config.get_value('REFERRAL_GOALS', [5, 10, 25])
        goals_str = ', '.join(str(x) for x in goals) if goals else '-'

        await update.message.reply_text(
            get_text('referral_message', lang, link=link, count=count, goals=goals_str),
            disable_web_page_preview=True,
        )

    async def cmd_setrefgoals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin: configure referral milestone goals."""
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return
        if not context.args:
            await update.message.reply_text("Uso: /setrefgoals 5 10 25")
            return
        goals = []
        for a in context.args:
            try:
                n = int(a)
                if n > 0:
                    goals.append(n)
            except Exception:
                pass
        goals = sorted(set(goals))
        if not goals:
            await update.message.reply_text("Nenhuma meta válida.")
            return
        config.set_value('REFERRAL_GOALS', goals)
        await update.message.reply_text(f"✅ Metas atualizadas: {', '.join(map(str, goals))}")

    async def cmd_reftop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin: show top referrers."""
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return
        top = self.referrals.get_top_referrers(limit=10)
        if not top:
            await update.message.reply_text("Ainda não há convites registrados.")
            return
        lines = ["🏆 Top referrers:"]
        for i, (uid, cnt) in enumerate(top, 1):
            lines.append(f"{i}. {uid} — {cnt}")
        await update.message.reply_text("\n".join(lines))

    # -----------------------
    # Broadcast (admin)
    # -----------------------
    async def bc_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        keyboard = [
            [InlineKeyboardButton("📣 VIP", callback_data="bc:target:vip"), InlineKeyboardButton("🆓 FREE", callback_data="bc:target:free")],
            [InlineKeyboardButton("📣 VIP + 🆓 FREE", callback_data="bc:target:both")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="bc:cancel")],
        ]
        await update.message.reply_text("Selecione o destino do disparo:", reply_markup=InlineKeyboardMarkup(keyboard))
        return BC_TARGET

    async def bc_start_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if not query.from_user or query.from_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        keyboard = [
            [InlineKeyboardButton("📣 VIP", callback_data="bc:target:vip"), InlineKeyboardButton("🆓 FREE", callback_data="bc:target:free")],
            [InlineKeyboardButton("📣 VIP + 🆓 FREE", callback_data="bc:target:both")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="bc:cancel")],
        ]
        await query.edit_message_text("Selecione o destino do disparo:", reply_markup=InlineKeyboardMarkup(keyboard))
        return BC_TARGET

    async def bc_choose_target(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if not query.from_user or query.from_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        data = query.data
        target = data.split(':')[-1]
        context.user_data['bc_target'] = target
        await query.edit_message_text(
            f"✅ Destino selecionado: {target.upper()}\n\nAgora envie a mensagem (texto ou mídia) que deseja disparar.\n\n/cancel para cancelar."
        )
        return BC_CONTENT

    async def bc_capture_content(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        msg = update.message
        context.user_data['bc_source_chat_id'] = msg.chat_id
        context.user_data['bc_source_message_id'] = msg.message_id
        context.user_data['bc_is_text'] = bool(msg.text and not msg.entities)
        context.user_data['bc_text'] = msg.text if msg.text else None

        keyboard = [
            [InlineKeyboardButton("✅ Sim", callback_data="bc:btn:yes"), InlineKeyboardButton("❌ Não", callback_data="bc:btn:no")],
            [InlineKeyboardButton("Cancelar", callback_data="bc:cancel")],
        ]
        await msg.reply_text("Deseja adicionar um botão (link) na mensagem?", reply_markup=InlineKeyboardMarkup(keyboard))
        return BC_BTN_Q

    async def bc_button_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if not query.from_user or query.from_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        choice = query.data.split(':')[-1]
        if choice == 'no':
            context.user_data['bc_btn_text'] = None
            context.user_data['bc_btn_url'] = None
            return await self._bc_ask_confirm(query, context)
        await query.edit_message_text("Ok! Envie o TEXTO do botão (ex: 'Entrar no VIP'): ")
        return BC_BTN_TEXT

    async def bc_button_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        txt = (update.message.text or '').strip()
        if not txt:
            await update.message.reply_text("Texto inválido. Envie novamente:")
            return BC_BTN_TEXT
        context.user_data['bc_btn_text'] = txt
        await update.message.reply_text("Agora envie o LINK (URL) do botão:")
        return BC_BTN_URL

    async def bc_button_url(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        url = (update.message.text or '').strip()
        if not (url.startswith('http://') or url.startswith('https://') or url.startswith('tg://')):
            await update.message.reply_text("URL inválida. Envie um link começando com http:// ou https://")
            return BC_BTN_URL
        context.user_data['bc_btn_url'] = url
        # Confirm
        return await self._bc_ask_confirm(update.message, context)

    async def _bc_ask_confirm(self, query, context: ContextTypes.DEFAULT_TYPE):
        target = context.user_data.get('bc_target', 'both')
        btn_text = context.user_data.get('bc_btn_text')
        btn_url = context.user_data.get('bc_btn_url')
        summary = f"✅ Pronto para disparar\n\nDestino: {target.upper()}\nBotão: {'SIM' if btn_text and btn_url else 'NÃO'}"
        keyboard = [
            [InlineKeyboardButton("🚀 Enviar", callback_data="bc:confirm:send"), InlineKeyboardButton("❌ Cancelar", callback_data="bc:cancel")],
        ]
        if hasattr(query, "edit_message_text"):
            await query.edit_message_text(summary, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard))
        return BC_CONFIRM

    async def bc_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        if not query.from_user or query.from_user.id != config.ADMIN_ID:
            return ConversationHandler.END
        if query.data != 'bc:confirm:send':
            return ConversationHandler.END

        target = context.user_data.get('bc_target', 'both')
        src_chat_id = context.user_data.get('bc_source_chat_id')
        src_msg_id = context.user_data.get('bc_source_message_id')
        text = context.user_data.get('bc_text')
        btn_text = context.user_data.get('bc_btn_text')
        btn_url = context.user_data.get('bc_btn_url')

        reply_markup = None
        if btn_text and btn_url:
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(btn_text, url=btn_url)]])

        targets = []
        if target in ('vip', 'both'):
            if config.VIP_CHANNEL_ID:
                targets.append(config.VIP_CHANNEL_ID)
        if target in ('free', 'both'):
            for k in ('FREE_CHANNEL_PT_ID', 'FREE_CHANNEL_ES_ID', 'FREE_CHANNEL_EN_ID'):
                cid = config.get_value(k)
                if cid:
                    targets.append(cid)

        sent = 0
        for cid in targets:
            try:
                if text is not None and src_msg_id is None:
                    await context.bot.send_message(chat_id=cid, text=text, reply_markup=reply_markup, disable_web_page_preview=True)
                else:
                    await context.bot.copy_message(chat_id=cid, from_chat_id=src_chat_id, message_id=src_msg_id, reply_markup=reply_markup)
                sent += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Broadcast send error to {cid}: {e}")

        context.user_data.pop('bc_target', None)
        context.user_data.pop('bc_source_chat_id', None)
        context.user_data.pop('bc_source_message_id', None)
        context.user_data.pop('bc_text', None)
        context.user_data.pop('bc_btn_text', None)
        context.user_data.pop('bc_btn_url', None)

        await query.edit_message_text(f"✅ Disparo finalizado. Enviado para {sent} destino(s).")
        return ConversationHandler.END

    async def bc_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message:
            await update.message.reply_text("Cancelado.")
        return ConversationHandler.END

    async def bc_cancel_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        try:
            await query.edit_message_text("Cancelado.")
        except Exception:
            pass
        return ConversationHandler.END


    # ---------- Auto previews (admin) ----------
    def _auto_task_running(self) -> bool:
        return self._auto_previews_task is not None and not self._auto_previews_task.done()

    def _wake_auto_task(self) -> None:
        try:
            self._auto_previews_stop_event.set()
        except Exception:
            pass

    async def _sleep_or_wake(self, seconds: float) -> None:
        """Sleep, but wake early when config changes or stop is requested."""
        try:
            await asyncio.wait_for(self._auto_previews_stop_event.wait(), timeout=max(0.0, float(seconds)))
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass
        try:
            self._auto_previews_stop_event.clear()
        except Exception:
            pass

    def _start_auto_previews_task(self) -> None:
        """Start background loop for auto previews (works even without PTB JobQueue extras)."""
        if not self.app:
            return
        if self._auto_task_running():
            self._wake_auto_task()
            return
        try:
            self._auto_previews_stop_event.clear()
        except Exception:
            pass
        self._auto_previews_task = asyncio.create_task(self._auto_previews_loop(), name="auto_previews_loop")

    def _stop_auto_previews_task(self) -> None:
        try:
            self._wake_auto_task()
            if self._auto_previews_task and not self._auto_previews_task.done():
                self._auto_previews_task.cancel()
        except Exception:
            pass
        self._auto_previews_task = None

    async def _auto_previews_loop(self) -> None:
        """Background loop that periodically sends auto previews based on DB config."""
        logger.info("Auto previews loop started")
        while True:
            try:
                cfg = self.preview_db.get_auto_config() or {}
                if int(cfg.get("enabled", 0)) != 1:
                    logger.info("Auto previews loop exiting (disabled)")
                    return

                interval_minutes = int(cfg.get("interval_minutes", 1440) or 1440)

                # Run one cycle
                await self.auto_previews_job(cfg_override=cfg)

                # Sleep until next run (or wake early)
                await self._sleep_or_wake(interval_minutes * 60.0)
            except asyncio.CancelledError:
                logger.info("Auto previews loop cancelled")
                return
            except Exception as e:
                logger.exception("Auto previews loop error: %s", e)
                await self._sleep_or_wake(10.0)


    # -----------------
    # VIP feed module (periodic VIP seeding)
    # -----------------
    def _vip_feed_task_running(self) -> bool:
        try:
            return bool(self._vip_feed_task and not self._vip_feed_task.done())
        except Exception:
            return False

    def _start_vip_feed_task(self) -> None:
        if not self.app:
            return
        if self._vip_feed_task_running():
            return
        try:
            self._vip_feed_stop_event.clear()
        except Exception:
            pass
        self._vip_feed_task = asyncio.create_task(self._vip_feed_loop(), name="vip_feed_loop")

    def _stop_vip_feed_task(self) -> None:
        try:
            if self._vip_feed_task and not self._vip_feed_task.done():
                self._vip_feed_task.cancel()
        except Exception:
            pass
        self._vip_feed_task = None

    def _coomer_creator_score(self, creator: dict) -> float:
        """Best-effort popularity score for creator objects returned by /api/v1/creators."""
        score = 0.0
        if not isinstance(creator, dict):
            return score
        # Common-ish numeric keys (varies per backend/version)
        for k in (
            "favorite_count",
            "favorites",
            "favorited",
            "followers",
            "subscribers",
            "posts",
            "post_count",
            "updated",
            "updated_at",
        ):
            v = creator.get(k)
            if v is None:
                continue
            try:
                score += float(v)
            except Exception:
                # Some fields are timestamps/strings; ignore
                continue
        return score

    async def _coomer_get_top_creators(self, fetcher: MediaFetcher, top_n: int = 100) -> list[dict]:
        """Return (best-effort) top creators list, cached for a short time."""
        now = time.time()
        try:
            cached = self._vip_feed_top_cache
            if cached and (now - float(cached.get("ts", 0.0))) < 1800 and cached.get("creators"):
                creators = cached.get("creators")
                if isinstance(creators, list) and creators:
                    return creators[:top_n]
        except Exception:
            pass

        creators = []
        try:
            creators = await fetcher._get_creators_list()
        except Exception:
            creators = []

        if not creators:
            return []

        # If API already returns in popularity order, this still works.
        try:
            creators_sorted = sorted(creators, key=lambda c: self._coomer_creator_score(c), reverse=True)
        except Exception:
            creators_sorted = creators

        creators_sorted = creators_sorted[: max(10, min(int(top_n or 100), 500))]
        try:
            self._vip_feed_top_cache = {"ts": now, "creators": creators_sorted}
        except Exception:
            pass
        return creators_sorted

    async def vip_feed_job(self) -> None:
        """One cycle: (1) respect auto previews (if enabled) and (2) seed VIP with a random Coomer page."""
        async with self._vip_feed_lock:
            cfg = self.preview_db.get_vip_feed_config() or {}
            if not cfg or int(cfg.get("enabled", 0)) != 1:
                return

            interval_minutes = int(cfg.get("interval_minutes", 60) or 60)
            page_items = int(cfg.get("page_items", 10) or 10)
            top_n = int(cfg.get("top_n", 100) or 100)

            # 1) Auto previews (if enabled) still runs using its own config
            try:
                await self.auto_previews_job(cfg_override=self.preview_db.get_auto_config())
            except Exception as e:
                logger.warning("VIP feed: auto previews step failed (ignored): %s", e)

            # 2) VIP seeding
            if not self.app:
                return
            if not self.uploader:
                try:
                    self.uploader = TelegramUploader(self.app.bot)
                except Exception:
                    return
            vip_chat_id = config.get_value("VIP_CHANNEL_ID", getattr(config, "VIP_CHANNEL_ID", 0))
            if not vip_chat_id:
                logger.warning("VIP feed: VIP_CHANNEL_ID not configured")
                return

            # Conservative offsets (Coomer uses ?o= offset in posts)
            offsets = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450]

            try:
                async with MediaFetcher() as fetcher:
                    top_creators = await self._coomer_get_top_creators(fetcher, top_n=top_n)
                    if not top_creators:
                        logger.warning("VIP feed: no creators available")
                        return

                    creator = random.choice(top_creators)
                    creator_name = creator.get("name") or "unknown"

                    media_items = []
                    # Try a few offsets until we get something
                    for _ in range(4):
                        off = random.choice(offsets)
                        try:
                            media_items = await fetcher.fetch_posts_paged(creator, offset=off, smart_sort=True)
                        except Exception as e:
                            logger.warning("VIP feed: fetch_posts_paged failed (%s): %s", creator_name, e)
                            media_items = []
                        if media_items:
                            break

                    if not media_items:
                        logger.warning("VIP feed: no media items for creator=%s", creator_name)
                        return

                    # Upload at most N items from that page
                    send_items = media_items[: max(1, min(int(page_items), 50))]
                    sent = 0
                    for item in send_items:
                        try:
                            # download (fetcher enforces MAX_TG_UPLOAD_MB guard)
                            try:
                                await fetcher.download_media(item)
                            except TooLargeMedia:
                                continue

                            caption = f"🔥 {creator_name}"
                            ok = await self.uploader.upload_and_cleanup(
                                media_item=item,
                                channel_id=int(vip_chat_id),
                                caption=caption,
                                model_name=str(creator_name),
                            )
                            if ok:
                                sent += 1
                            await asyncio.sleep(2.0)
                        except Exception as e:
                            logger.warning("VIP feed: send item failed (%s): %s", creator_name, e)
                            continue

                    logger.info(
                        "VIP feed: cycle done interval=%sm top_n=%s page_items=%s creator=%s sent=%s",
                        interval_minutes,
                        top_n,
                        page_items,
                        creator_name,
                        sent,
                    )
            except Exception as e:
                logger.exception("VIP feed job error: %s", e)


    async def _vip_feed_loop(self) -> None:
        logger.info("VIP feed loop started")
        while True:
            try:
                cfg = self.preview_db.get_vip_feed_config() or {}
                if int(cfg.get("enabled", 0)) != 1:
                    logger.info("VIP feed loop exiting (disabled)")
                    return
                interval_minutes = int(cfg.get("interval_minutes", 60) or 60)
                await self.vip_feed_job()
                await self._sleep_or_wake(interval_minutes * 60.0)
            except asyncio.CancelledError:
                logger.info("VIP feed loop cancelled")
                return
            except Exception as e:
                logger.exception("VIP feed loop error: %s", e)
                await self._sleep_or_wake(10.0)

    async def cmd_autopreviews_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return

        # /autopreviews_on <interval_min> <qty> <model|all> <dest:free|private|both> [free_target]
        # free_target: all | pt | es | en | <chat_id>
        args = context.args or []
        if len(args) < 4:
            await update.message.reply_text(
                "Uso: /autopreviews_on <interval_min> <qty> <model|all> <dest:free|private|both> [free_target]\n\nfree_target: all | pt | es | en | <chat_id>",
                parse_mode=None,
            )
            return

        try:
            interval_minutes = max(1, int(args[0]))
        except Exception:
            interval_minutes = 1440
        try:
            qty = max(1, min(int(args[1]), 50))
        except Exception:
            qty = 5

        model = (args[2] or "all").strip().lower() or "all"
        dest = (args[3] or "free").strip().lower()
        send_to_free = 1 if dest in ("free", "both") else 0
        send_to_private = 1 if dest in ("private", "both") else 0

        free_mode = "all"
        free_chat_id = None
        if send_to_free and len(args) >= 5:
            tgt = (args[4] or "all").strip().lower()
            if tgt in ("all", "todos"):
                free_mode = "all"
                free_chat_id = None
            elif tgt in ("pt", "br"):
                free_mode = "single"
                free_chat_id = config.get_value("FREE_CHANNEL_PT_ID")
            elif tgt == "es":
                free_mode = "single"
                free_chat_id = config.get_value("FREE_CHANNEL_ES_ID")
            elif tgt == "en":
                free_mode = "single"
                free_chat_id = config.get_value("FREE_CHANNEL_EN_ID")
            else:
                # allow direct chat_id
                try:
                    free_mode = "single"
                    free_chat_id = int(tgt)
                except Exception:
                    free_mode = "all"
                    free_chat_id = None

        # Use current chat as admin private destination
        admin_chat_id = int(update.effective_chat.id) if update.effective_chat else None

        ok = self.preview_db.set_auto_config(
            enabled=1,
            interval_minutes=interval_minutes,
            qty=qty,
            model=model,
            send_to_free=send_to_free,
            send_to_private=send_to_private,
            admin_chat_id=admin_chat_id if send_to_private else None,
            free_mode=free_mode,
            free_chat_id=free_chat_id,
        )
        if ok:
            self._start_auto_previews_task()
            ft = "all" if free_mode == "all" or not free_chat_id else str(free_chat_id)
            await self._safe_send_message(update.effective_chat.id, f"✅ Auto previews ON. interval={interval_minutes}m qty={qty} model={model} dest={dest} free_target={ft}")
        else:
            await self._safe_send_message(update.effective_chat.id, "⚠️ Não foi possível salvar config (ver logs).")

    async def cmd_autopreviews_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or update.effective_user.id != config.ADMIN_ID:
            return

        self.preview_db.set_auto_config(
            enabled=0,
            interval_minutes=1440,
            qty=5,
            model="all",
            send_to_free=1,
            send_to_private=0,
            admin_chat_id=None,
        )
        # JobQueue may not be available (PTB installed without extras). Use our internal loop.
        self._stop_auto_previews_task()
        await self._safe_send_message(update.effective_chat.id, "✅ Auto previews OFF.")


    async def cmd_autopreviews_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
        cfg = self.preview_db.get_auto_config()
        if not cfg:
            await self._safe_send_message(update.effective_chat.id, "Auto previews config: not set.")
            return

        enabled = cfg.get("enabled")
        interval_minutes = cfg.get("interval_minutes")
        qty = cfg.get("qty")
        model = cfg.get("model")
        send_to_free = int(cfg.get("send_to_free", 0) or 0)
        send_to_private = int(cfg.get("send_to_private", 0) or 0)
        free_mode = (cfg.get("free_mode") or cfg.get("free_target_mode") or "all").strip().lower()
        free_chat_id = cfg.get("free_chat_id") if cfg.get("free_chat_id") is not None else cfg.get("free_target_chat_id")
        admin_chat_id = cfg.get("admin_chat_id")
        if send_to_free and send_to_private:
            dest = "both"
        elif send_to_private:
            dest = "private"
        else:
            dest = "free"

        status = (
            "📌 Auto previews status\n"
            f"enabled: {enabled}\n"
            f"interval_minutes: {interval_minutes}\n"
            f"qty: {qty}\n"
            f"model: {model}\n"
            f"dest: {dest}\n"
            f"free_target: {('all' if free_mode != 'single' or not free_chat_id else str(free_chat_id))}\n"
            f"admin_chat_id: {admin_chat_id}"
        )
        await self._safe_send_message(update.effective_chat.id, status)


    async def cmd_vipfeed_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Enable VIP feed module.

        Usage: /vipfeed_on <interval_min> <page_items> [top_n]
        """
        if not self._is_admin(update):
            return
        args = context.args or []
        if len(args) < 2:
            await self._safe_send_message(
                update.effective_chat.id,
                "Uso: /vipfeed_on <interval_min> <page_items> [top_n]\nEx.: /vipfeed_on 60 10 100",
            )
            return
        try:
            interval_minutes = max(1, int(args[0]))
        except Exception:
            interval_minutes = 60
        try:
            page_items = max(1, min(int(args[1]), 50))
        except Exception:
            page_items = 10
        try:
            top_n = max(10, min(int(args[2]), 500)) if len(args) >= 3 else 100
        except Exception:
            top_n = 100

        ok = self.preview_db.set_vip_feed_config(
            enabled=1,
            interval_minutes=interval_minutes,
            page_items=page_items,
            top_n=top_n,
        )
        if ok:
            self._start_vip_feed_task()
            await self._safe_send_message(
                update.effective_chat.id,
                f"✅ VIP feed ON. interval={interval_minutes}m page_items={page_items} top_n={top_n}",
            )
        else:
            await self._safe_send_message(update.effective_chat.id, "⚠️ Não foi possível salvar vipfeed config (ver logs).")


    async def cmd_vipfeed_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
        self.preview_db.set_vip_feed_config(enabled=0, interval_minutes=60, page_items=10, top_n=100)
        self._stop_vip_feed_task()
        await self._safe_send_message(update.effective_chat.id, "✅ VIP feed OFF.")


    async def cmd_vipfeed_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
        cfg = self.preview_db.get_vip_feed_config() or {}
        status = (
            "📌 VIP feed status\n"
            f"enabled: {cfg.get('enabled')}\n"
            f"interval_minutes: {cfg.get('interval_minutes')}\n"
            f"page_items: {cfg.get('page_items')}\n"
            f"top_n: {cfg.get('top_n')}"
        )
        await self._safe_send_message(update.effective_chat.id, status)

    async def auto_previews_job(self, context: ContextTypes.DEFAULT_TYPE = None, cfg_override: dict | None = None):
        """Send random previews on a schedule.

        Works with PTB JobQueue (if installed) and also with our internal asyncio loop.
        """
        async with self._auto_previews_lock:
            cfg = cfg_override or self.preview_db.get_auto_config()
            if not cfg or int(cfg.get("enabled", 0)) != 1:
                return

            qty = int(cfg.get("qty", 5) or 5)
            model = cfg.get("model") or "random"
            # Backward compatibility: older builds stored different config keys.
            # Current config comes from PreviewIndexManager.set_auto_config().
            send_to_free = int(cfg.get("send_to_free", 0) or 0)
            send_to_private = int(cfg.get("send_to_private", 0) or 0)
            free_mode = (cfg.get("free_mode") or cfg.get("free_target_mode") or "all").strip().lower()
            free_chat_id = cfg.get("free_chat_id") if cfg.get("free_chat_id") is not None else cfg.get("free_target_chat_id")
            admin_chat_id = cfg.get("admin_chat_id")

            targets: list[int] = []
            # Private/admin destination
            if send_to_private and admin_chat_id:
                targets.append(int(admin_chat_id))
            # Free destinations
            if send_to_free:
                if free_mode == "single" and free_chat_id:
                    targets.append(int(free_chat_id))
                else:
                    targets.extend([int(x) for x in (config.FREE_CHANNELS or [])])

            # De-dup while preserving order
            seen = set()
            targets = [t for t in targets if not (t in seen or seen.add(t))]

            if not targets:
                return

            bot = None
            try:
                bot = context.bot if context else (self.app.bot if self.app else None)
            except Exception:
                bot = self.app.bot if self.app else None
            if bot is None:
                return

            for chat_id in targets:
                try:
                    await self._send_random_previews(bot=bot, model=model, qty=qty, dest_chat_ids=[int(chat_id)])
                except Exception as e:
                    logger.exception("Auto previews send error chat_id=%s: %s", chat_id, e)



    def setup_handlers(self):
        """Register all command/callback handlers.

        This method is required by VIPBot.run() and must exist in production.
        It preserves the functional behavior from the reference build and includes
        the extra handlers present in this version (auto previews, VIP indexing, etc.).
        """
        # VIP channel auto-index (silent)
        try:
            self.app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.ALL, self.on_vip_channel_post))
        except Exception:
            # Older PTB filter compatibility fallback
            self.app.add_handler(MessageHandler(filters.ALL, self.on_vip_channel_post))

        # Core
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("search", self.cmd_search))
        self.app.add_handler(CommandHandler("help", cmd_help))

        # Referral
        self.app.add_handler(CommandHandler("ref", self.cmd_ref))
        self.app.add_handler(CommandHandler("setrefgoals", self.cmd_setrefgoals))
        self.app.add_handler(CommandHandler("reftop", self.cmd_reftop))

        # Manual previews (admin) + random previews (admin)
        previews_conv = ConversationHandler(
            entry_points=[
                CommandHandler("sendpreviews", self.pv_start),
                CallbackQueryHandler(self.pv_start, pattern="^admin:previews$"),
            ],
            states={
                PV_MENU: [CallbackQueryHandler(self.pv_menu_choice, pattern="^(pv:|admin:menu$)")],
                PV_SEND_MODEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.pv_receive_model)],
                PV_INDEX_MODEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.pv_index_model)],
                PV_INDEX_COLLECT: [
                    CommandHandler("done", self.pv_index_done),
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.pv_index_collect),
                ],
                # Random previews flow
                PV_RAND_MODEL: [
                    CallbackQueryHandler(self.pv_rand_model_cb, pattern="^pv:rand:"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.pv_rand_model_text),
                ],
                PV_RAND_QTY: [
                    CallbackQueryHandler(self.pv_rand_qty_cb, pattern="^pv:qty:"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.pv_rand_qty_text),
                ],
                PV_RAND_DEST: [
                    CallbackQueryHandler(self.pv_rand_dest_cb, pattern="^pv:dest:"),
                ],
            },
            fallbacks=[CommandHandler("cancel", self.pv_cancel)],
            per_user=True,
            per_chat=True,
        )
        self.app.add_handler(previews_conv)

        # Broadcast (admin)
        broadcast_conv = ConversationHandler(
            entry_points=[
                CommandHandler("broadcast", self.bc_start),
                CallbackQueryHandler(self.bc_start_cb, pattern="^admin:broadcast$"),
            ],
            states={
                BC_TARGET: [CallbackQueryHandler(self.bc_choose_target, pattern="^bc:target:")],
                BC_CONTENT: [
                    MessageHandler(filters.ALL & ~filters.COMMAND, self.bc_capture_content),
                    CommandHandler("cancel", self.bc_cancel),
                ],
                BC_BTN_Q: [CallbackQueryHandler(self.bc_button_choice, pattern="^bc:btn:")],
                BC_BTN_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.bc_button_text)],
                BC_BTN_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.bc_button_url)],
                BC_CONFIRM: [CallbackQueryHandler(self.bc_confirm, pattern="^bc:confirm:")],
            },
            fallbacks=[
                CommandHandler("cancel", self.bc_cancel),
                CallbackQueryHandler(self.bc_cancel_cb, pattern="^bc:cancel$"),
            ],
            per_user=True,
            per_chat=True,
        )
        self.app.add_handler(broadcast_conv)

        # Auto previews (admin)
        self.app.add_handler(CommandHandler("autopreviews_on", self.cmd_autopreviews_on))
        self.app.add_handler(CommandHandler("autopreviews_off", self.cmd_autopreviews_off))
        self.app.add_handler(CommandHandler("autopreviews_status", self.cmd_autopreviews_status))

        # VIP feed module (admin)
        self.app.add_handler(CommandHandler("vipfeed_on", self.cmd_vipfeed_on))
        self.app.add_handler(CommandHandler("vipfeed_off", self.cmd_vipfeed_off))
        self.app.add_handler(CommandHandler("vipfeed_status", self.cmd_vipfeed_status))

        # Admin config commands
        self.app.add_handler(CommandHandler("setvip", cmd_setvip))
        self.app.add_handler(CommandHandler("setfreept", cmd_setfreept))
        self.app.add_handler(CommandHandler("setfreees", cmd_setfreees))
        self.app.add_handler(CommandHandler("setfreeen", cmd_setfreeen))
        self.app.add_handler(CommandHandler("setsubbot_pt", cmd_setsubbot_pt))
        self.app.add_handler(CommandHandler("setsubbot_es", cmd_setsubbot_es))
        self.app.add_handler(CommandHandler("setsubbot_en", cmd_setsubbot_en))
        self.app.add_handler(CommandHandler("setsource", cmd_setsource))
        self.app.add_handler(CommandHandler("setpreview", cmd_setpreview))
        self.app.add_handler(CommandHandler("setpreviewlimit", cmd_setpreviewlimit))
        self.app.add_handler(CommandHandler("setlang", cmd_setlang))
        self.app.add_handler(CommandHandler("stats", cmd_stats))
        self.app.add_handler(CommandHandler("restart", cmd_restart))

        # Whitelist management
        self.app.add_handler(CommandHandler("addadmin", cmd_addadmin))
        self.app.add_handler(CommandHandler("removeadmin", cmd_removeadmin))
        self.app.add_handler(CommandHandler("listadmins", cmd_listadmins))

        # Callback queries (menus, pagination, etc.)
        self.app.add_handler(CallbackQueryHandler(self.on_callback_query))

        # Startup log: handler count
        try:
            total = 0
            for grp, hs in (self.app.handlers or {}).items():
                total += len(hs or [])
            logger.info("Handlers registered: %s (groups=%s)", total, list((self.app.handlers or {}).keys()))
        except Exception:
            logger.info("Handlers registered (count unavailable)")

    async def post_init(self, application: Application):
        """Post-initialization callback (called by PTB after initialization)."""
        try:
            import telegram
            logger.info("python-telegram-bot version: %s", getattr(telegram, "__version__", "unknown"))
        except Exception:
            pass

        # JobQueue detection (we do NOT depend on it)
        jobq = None
        try:
            # Avoid accessing application.job_queue property (emits PTB warning when extra not installed)
            jobq = getattr(application, "_job_queue", None)
        except Exception:
            jobq = None
        logger.info("JobQueue available: %s", "yes" if jobq else "no")
        logger.info("Scheduler mode: %s", "jobqueue" if jobq else "asyncio-fallback")

        # Uploader ready
        try:
            self.uploader = TelegramUploader(application.bot)
        except Exception as e:
            logger.warning("Uploader init failed (continuing): %s", e)

        # Auto previews background loop (asyncio fallback)
        try:
            cfg = self.preview_db.get_auto_config()
            if cfg and cfg.get("enabled"):
                self._start_auto_previews_task()
                logger.info("Auto previews: enabled (background loop started)")
            else:
                logger.info("Auto previews: disabled")
        except Exception as e:
            logger.warning("Auto previews init failed (ignored): %s", e)

        # VIP feed background loop (asyncio fallback)
        try:
            vcfg = self.preview_db.get_vip_feed_config()
            if vcfg and int(vcfg.get("enabled", 0)) == 1:
                self._start_vip_feed_task()
                logger.info("VIP feed: enabled (background loop started)")
            else:
                logger.info("VIP feed: disabled")
        except Exception as e:
            logger.warning("VIP feed init failed (ignored): %s", e)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Central error handler - must never raise."""
        try:
            err = getattr(context, "error", None)
            if isinstance(err, RetryAfter):
                ra = float(getattr(err, "retry_after", 0) or 0)
                # best-effort chat_id for per-chat cooldown
                chat_id = None
                try:
                    if update and update.effective_chat:
                        chat_id = update.effective_chat.id
                except Exception:
                    chat_id = None
                self._tg_register_cooldown(ra, chat_id=chat_id)
                logger.warning("Handler raised RetryAfter (cooldown registered) retry_after=%s update=%s", getattr(err, "retry_after", None), type(update).__name__)
                return
            if isinstance(err, (BadRequest, Forbidden, TelegramError)):
                logger.warning("Telegram error in handler: %s", err)
                return
            if isinstance(err, (TimedOut, NetworkError)):
                logger.warning("Network error in handler: %s", err)
                return
            logger.exception("Unhandled exception in handler: %s", err)
        except Exception as e:
            logger.warning("error_handler failed (ignored): %s", e)

    def run(self):
        """Run the bot"""
        if not config.validate():
            logger.error("Invalid configuration. Please check your .env file")
            return
        
        self.app = Application.builder().token(config.BOT_TOKEN).build()
        self.setup_handlers()
        self.app.add_error_handler(self.error_handler)
        self.app.post_init = self.post_init
        
        logger.info("Starting bot...")
        self.app.run_polling(allowed_updates=Update.ALL_TYPES)



def main():
    """Main entry point"""
    logger.info("=" * 50)
    logger.info("Telegram VIP Media Bot v2.1")
    logger.info("=" * 50)
    
    bot = VIPBot()
    bot.run()


if __name__ == "__main__":
    main()