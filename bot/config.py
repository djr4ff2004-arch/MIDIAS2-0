"""
Configuration module for Telegram VIP Bot
Handles all configuration settings and environment variables
"""

import os
import json
import logging
import tempfile
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Optional Postgres persistence
try:
    import psycopg2
    import psycopg2.extras
except Exception:  # pragma: no cover
    psycopg2 = None


# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class Config:
    """Configuration manager for the bot"""
    
    def __init__(self):
        # Bot settings
        self.BOT_TOKEN = os.getenv("BOT_TOKEN")
        self.ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
        
        # Authorized users whitelist (starts with main admin)
        self.AUTHORIZED_USERS = [self.ADMIN_ID] if self.ADMIN_ID else []
        
        # Channel IDs
        self.VIP_CHANNEL_ID = int(os.getenv("VIP_CHANNEL_ID", 0))
        self.FREE_CHANNEL_PT_ID = int(os.getenv("FREE_CHANNEL_PT_ID", 0))
        self.FREE_CHANNEL_ES_ID = int(os.getenv("FREE_CHANNEL_ES_ID", 0))
        self.FREE_CHANNEL_EN_ID = int(os.getenv("FREE_CHANNEL_EN_ID", 0))
        
        # Subscription bot links per language
        self.SUB_BOT_LINK_PT = os.getenv("SUB_BOT_LINK_PT", "https://t.me/YourBotPT")
        self.SUB_BOT_LINK_ES = os.getenv("SUB_BOT_LINK_ES", "https://t.me/YourBotES")
        self.SUB_BOT_LINK_EN = os.getenv("SUB_BOT_LINK_EN", "https://t.me/YourBotEN")
        
        # Media sources
        sources_str = os.getenv("MEDIA_SOURCES", "https://coomer.st,https://picazor.com")
        self.MEDIA_SOURCES = [s.strip() for s in sources_str.split(",")]
        
        # Preview settings
        self.PREVIEW_TYPE = os.getenv("PREVIEW_TYPE", "none") # 'none' means no blur/watermark
        self.PREVIEW_QUALITY = int(os.getenv("PREVIEW_QUALITY", 80))
        self.PREVIEW_LIMIT = int(os.getenv("PREVIEW_LIMIT", 3)) # Max previews per model per channel
        
        # Upload settings
        self.MAX_FILES_PER_BATCH = int(os.getenv("MAX_FILES_PER_BATCH", 10))
        self.AUTO_POST_INTERVAL = int(os.getenv("AUTO_POST_INTERVAL", 300))
        
        # Language
        self.DEFAULT_LANG = os.getenv("DEFAULT_LANG", "pt")

        # Public referral mode (allow /start and /ref for non-whitelisted users)
        self.PUBLIC_REFERRAL_MODE = os.getenv("PUBLIC_REFERRAL_MODE", "false").strip().lower() in ("1", "true", "yes", "y")

        # Optional FREE join links (recommended for public onboarding)
        self.FREE_JOIN_LINK_PT = os.getenv("FREE_JOIN_LINK_PT", "").strip()
        self.FREE_JOIN_LINK_ES = os.getenv("FREE_JOIN_LINK_ES", "").strip()
        self.FREE_JOIN_LINK_EN = os.getenv("FREE_JOIN_LINK_EN", "").strip()
        
        # Database
        self.DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bot_data.db")
        
        # Runtime config file
        self.CONFIG_FILE = "bot_config.json"
        self.runtime_config = self._load_runtime_config()
        # If DATABASE_URL points to Postgres, load persisted config from DB (survives Railway restarts)
        self._load_postgres_runtime_config()
        
        # Load authorized users from runtime config
        saved_users = self.runtime_config.get("AUTHORIZED_USERS", [])
        if saved_users:
            self.AUTHORIZED_USERS = saved_users
        else:
            # Save initial admin to runtime config
            self.set_value("AUTHORIZED_USERS", self.AUTHORIZED_USERS)
    
    def _load_runtime_config(self) -> Dict[str, Any]:
        """Load runtime configuration from JSON file"""
        def _try_load(path: str) -> Dict[str, Any] | None:
            try:
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    with open(path, 'r', encoding='utf-8') as f:
                        return json.load(f)
            except Exception as e:
                logger.error(f"Error loading runtime config from {path}: {e}")
            return None

        # Prefer primary file, then fall back to a last-known-good backup.
        data = _try_load(self.CONFIG_FILE)
        if isinstance(data, dict):
            return data

        bak = self.CONFIG_FILE + ".bak"
        data = _try_load(bak)
        if isinstance(data, dict):
            logger.warning("Runtime config was invalid; recovered from backup: %s", bak)
            return data

        return {}
    
    def _save_runtime_config(self):
        """Save runtime configuration to JSON file"""
        # Atomic write to prevent partial/corrupted JSON when multiple async paths
        # write config close together. This was a real cause of "config reset".
        try:
            target = self.CONFIG_FILE
            bak = self.CONFIG_FILE + ".bak"

            # Best-effort: keep a last-known-good backup before overwriting.
            try:
                if os.path.exists(target) and os.path.getsize(target) > 0:
                    with open(target, "rb") as r, open(bak, "wb") as w:
                        w.write(r.read())
            except Exception:
                pass

            d = os.path.dirname(os.path.abspath(target)) or "."
            fd, tmp_path = tempfile.mkstemp(prefix=".bot_config_", suffix=".json", dir=d)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(self.runtime_config, f, indent=2, ensure_ascii=False)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        pass
                os.replace(tmp_path, target)
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error saving runtime config: {e}")
    

    # ---------------------------
    # Postgres-backed persistence
    # ---------------------------

    def _is_postgres(self) -> bool:
        """Return True if DATABASE_URL points to Postgres."""
        url = (self.DATABASE_URL or "").strip().lower()
        return url.startswith("postgres://") or url.startswith("postgresql://")

    def _pg_connect(self):
        """Create a Postgres connection or return None."""
        if psycopg2 is None or not self._is_postgres():
            return None
        url = (self.DATABASE_URL or "").strip()
        # psycopg2 expects postgresql://, Railway sometimes provides postgres://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        try:
            conn = psycopg2.connect(url)
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Postgres connect failed: {e}")
            return None

    def _ensure_pg_tables(self, conn):
        """Ensure required tables exist."""
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bot_settings (
                        key TEXT PRIMARY KEY,
                        value JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f"Postgres table init failed: {e}")

    def _load_postgres_runtime_config(self):
        """Load runtime config values from Postgres into runtime_config."""
        if psycopg2 is None or not self._is_postgres():
            return
        conn = self._pg_connect()
        if conn is None:
            return
        try:
            self._ensure_pg_tables(conn)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT key, value FROM bot_settings")
                rows = cur.fetchall() or []
            # Merge DB values over file/env defaults
            for row in rows:
                k = row.get("key")
                v = row.get("value")
                if k:
                    self.runtime_config[k] = v
                    if hasattr(self, k):
                        try:
                            setattr(self, k, v)
                        except Exception:
                            pass
            # Persist merged view back to JSON file for visibility/debugging
            self._save_runtime_config()
        except Exception as e:
            logger.error(f"Postgres load failed: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _save_postgres_setting(self, key: str, value: Any):
        """Persist one setting into Postgres."""
        if psycopg2 is None or not self._is_postgres():
            return
        conn = self._pg_connect()
        if conn is None:
            return
        try:
            self._ensure_pg_tables(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bot_settings (key, value, updated_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value,
                        updated_at = now()
                    """,
                    (key, psycopg2.extras.Json(value)),
                )
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f"Postgres save failed for {key}: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def set_value(self, key: str, value: Any):
        """Set a configuration value at runtime"""
        self.runtime_config[key] = value
        self._save_runtime_config()
        # Persist to Postgres if configured
        self._save_postgres_setting(key, value)
        
        # Update instance attributes
        if hasattr(self, key):
            setattr(self, key, value)
    
    def get_value(self, key: str, default: Any = None) -> Any:
        """Get a configuration value"""
        return self.runtime_config.get(key, getattr(self, key, default))

    @property
    def FREE_CHANNELS(self) -> list[int]:
        """Return all configured FREE channel IDs.

        Some scheduler code paths reference `config.FREE_CHANNELS` (plural), while
        the canonical persisted settings are per-language keys:
        FREE_CHANNEL_PT_ID, FREE_CHANNEL_ES_ID, FREE_CHANNEL_EN_ID.
        This property keeps backward/forward compatibility without requiring
        callers to know the underlying key names.
        """
        ids = [
            self.get_value("FREE_CHANNEL_PT_ID", 0) or 0,
            self.get_value("FREE_CHANNEL_ES_ID", 0) or 0,
            self.get_value("FREE_CHANNEL_EN_ID", 0) or 0,
        ]
        out: list[int] = []
        for x in ids:
            try:
                xi = int(x)
                if xi:
                    out.append(xi)
            except Exception:
                continue
        return out
    
    def get_free_channel_by_lang(self, lang: str) -> Optional[int]:
        """Get FREE channel ID by language"""
        channels = {
            'pt': self.get_value("FREE_CHANNEL_PT_ID"),
            'es': self.get_value("FREE_CHANNEL_ES_ID"),
            'en': self.get_value("FREE_CHANNEL_EN_ID")
        }
        return channels.get(lang)

    def get_sub_link_by_lang(self, lang: str) -> str:
        """Get subscription bot link by language"""
        links = {
            'pt': self.get_value("SUB_BOT_LINK_PT"),
            'es': self.get_value("SUB_BOT_LINK_ES"),
            'en': self.get_value("SUB_BOT_LINK_EN")
        }
        return links.get(lang, self.get_value("SUB_BOT_LINK_PT"))
    
    def validate(self) -> bool:
        """Validate essential configuration"""
        if not self.BOT_TOKEN:
            logger.error("BOT_TOKEN is not set!")
            return False
        
        if not self.ADMIN_ID:
            logger.error("ADMIN_ID is not set!")
            return False
        
        return True
    
    def is_authorized(self, user_id: int) -> bool:
        """Check if user is authorized to use the bot"""
        return user_id in self.AUTHORIZED_USERS
    
    def add_authorized_user(self, user_id: int) -> bool:
        """Add user to authorized list"""
        if user_id not in self.AUTHORIZED_USERS:
            self.AUTHORIZED_USERS.append(user_id)
            self.set_value("AUTHORIZED_USERS", self.AUTHORIZED_USERS)
            logger.info(f"User {user_id} added to authorized list")
            return True
        return False
    
    def remove_authorized_user(self, user_id: int) -> bool:
        """Remove user from authorized list (cannot remove main admin)"""
        if user_id == self.ADMIN_ID:
            logger.warning(f"Cannot remove main admin {user_id}")
            return False
        
        if user_id in self.AUTHORIZED_USERS:
            self.AUTHORIZED_USERS.remove(user_id)
            self.set_value("AUTHORIZED_USERS", self.AUTHORIZED_USERS)
            logger.info(f"User {user_id} removed from authorized list")
            return True
        return False
    
    def get_authorized_users(self) -> list:
        """Get list of authorized users"""
        return self.AUTHORIZED_USERS.copy()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get configuration statistics"""
        return {
            "vip_channel": self.VIP_CHANNEL_ID,
            "free_channels": {
                "pt": self.FREE_CHANNEL_PT_ID,
                "es": self.FREE_CHANNEL_ES_ID,
                "en": self.FREE_CHANNEL_EN_ID
            },
            "media_sources": len(self.MEDIA_SOURCES),
            "preview_type": self.PREVIEW_TYPE,
            "max_batch": self.MAX_FILES_PER_BATCH,
            "auto_post_interval": self.AUTO_POST_INTERVAL
        }


# Global config instance
config = Config()
