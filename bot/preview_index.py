import logging
import os
import sqlite3
from contextlib import contextmanager

try:
    import psycopg2
except Exception:
    psycopg2 = None

logger = logging.getLogger(__name__)


class PreviewIndexManager:
    """Stores an index of VIP message IDs per model.

    When the admin forwards posts from the VIP channel to the bot,
    we capture the ORIGINAL source message_id (from the channel),
    and later use copy_message() to re-send those messages to FREE
    channels without downloading files again.
    """

    def __init__(self, database_url: str | None = None, db_path: str = "previews_index.db"):
        self.database_url = (database_url or "").strip()
        # Backward compatibility for SQLite: older builds used other filenames.
        # If the configured db_path doesn't exist (or is empty) but a legacy DB exists,
        # prefer the legacy so previously-indexed previews remain available after updates.
        self.db_path = db_path
        self.use_postgres = bool(self.database_url) and self.database_url.startswith(
            ("postgres://", "postgresql://")
        )
        if self.use_postgres and psycopg2 is None:
            logger.warning("Postgres URL provided but psycopg2 is not available; falling back to SQLite")
            self.use_postgres = False

        if not self.use_postgres:
            candidates = [
                self.db_path,
                "previews.db",  # legacy
                "previews_index.db",  # current default
            ]
            # Prefer an existing, non-empty database file.
            best = None
            best_size = -1
            for p in candidates:
                try:
                    size = os.path.getsize(p)
                except OSError:
                    size = -1
                if size > best_size:
                    best_size = size
                    best = p
            if best and best_size > 0 and best != self.db_path:
                logger.info("Using legacy previews DB file: %s", best)
                self.db_path = best

        self._init_storage()

    @contextmanager
    def _sqlite_conn(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _pg_conn(self):
        if not psycopg2:
            raise RuntimeError("psycopg2 not available")
        conn = psycopg2.connect(self.database_url)
        try:
            yield conn
        finally:
            conn.close()

    def _init_storage(self):
        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS preview_index (
                            model TEXT NOT NULL,
                            message_id BIGINT NOT NULL,
                            created_at TIMESTAMP DEFAULT NOW(),
                            PRIMARY KEY (model, message_id)
                        );
                        """
                    )
                    # Fallback storage when Telegram doesn't expose the forward origin chat/message_id
                    # (common when forwarding from groups).
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS preview_assets (
                            model TEXT NOT NULL,
                            media_type TEXT NOT NULL,
                            file_id TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT NOW(),
                            PRIMARY KEY (model, file_id)
                        );
                        """
                    )
                conn.commit()
        else:
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS preview_index (
                        model TEXT NOT NULL,
                        message_id INTEGER NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (model, message_id)
                    );
                    """
                )
                # Fallback storage when Telegram doesn't expose the forward origin chat/message_id.
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS preview_assets (
                        model TEXT NOT NULL,
                        media_type TEXT NOT NULL,
                        file_id TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (model, file_id)
                    );
                    """
                )
                conn.commit()

    def add(self, model: str, message_id: int) -> bool:
        """Returns True if inserted, False if already existed."""
        model = (model or "").strip().lower()
        if not model or not message_id:
            return False

        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO preview_index(model, message_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (model, int(message_id)),
                    )
                    inserted = cur.rowcount > 0
                conn.commit()
                return inserted

        with self._sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO preview_index(model, message_id) VALUES (?, ?);",
                (model, int(message_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def add_asset(self, model: str, media_type: str, file_id: str) -> bool:
        """Returns True if inserted, False if already existed."""
        model = (model or "").strip().lower()
        media_type = (media_type or "").strip().lower()
        file_id = (file_id or "").strip()
        if not model or not media_type or not file_id:
            return False

        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO preview_assets(model, media_type, file_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        (model, media_type, file_id),
                    )
                    inserted = cur.rowcount > 0
                conn.commit()
                return inserted

        with self._sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO preview_assets(model, media_type, file_id) VALUES (?, ?, ?);",
                (model, media_type, file_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def get_assets(self, model: str, limit: int = 40) -> list[tuple[str, str]]:
        model = (model or "").strip().lower()
        if not model:
            return []

        limit = max(1, min(int(limit), 200))
        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT media_type, file_id
                        FROM preview_assets
                        WHERE model = %s
                        ORDER BY created_at DESC
                        LIMIT %s;
                        """,
                        (model, limit),
                    )
                    rows = cur.fetchall()
                    return [(str(r[0]), str(r[1])) for r in rows]

        with self._sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT media_type, file_id
                FROM preview_assets
                WHERE model = ?
                ORDER BY created_at DESC
                LIMIT ?;
                """,
                (model, limit),
            )
            return [(str(r[0]), str(r[1])) for r in cur.fetchall()]

    def get_message_ids(self, model: str, limit: int = 40) -> list[int]:
        model = (model or "").strip().lower()
        if not model:
            return []

        limit = max(1, min(int(limit), 200))
        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT message_id
                        FROM preview_index
                        WHERE model = %s
                        ORDER BY created_at DESC
                        LIMIT %s;
                        """,
                        (model, limit),
                    )
                    rows = cur.fetchall()
                    return [int(r[0]) for r in rows]

        with self._sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT message_id
                FROM preview_index
                WHERE model = ?
                ORDER BY created_at DESC
                LIMIT ?;
                """,
                (model, limit),
            )
            return [int(r[0]) for r in cur.fetchall()]

    def count(self, model: str) -> int:
        model = (model or "").strip().lower()
        if not model:
            return 0

        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM preview_index WHERE model=%s;", (model,))
                    a = int(cur.fetchone()[0])
                    cur.execute("SELECT COUNT(*) FROM preview_assets WHERE model=%s;", (model,))
                    b = int(cur.fetchone()[0])
                    return a + b

        with self._sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM preview_index WHERE model=?;", (model,))
            a = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM preview_assets WHERE model=?;", (model,))
            b = int(cur.fetchone()[0])
            return a + b

    def find_models(self, partial: str, limit: int = 10) -> list[str]:
        """Find indexed model keys that contain `partial` (case-insensitive)."""
        partial = (partial or "").strip().lower()
        if not partial:
            return []
        limit = max(1, min(int(limit or 10), 50))

        if self.use_postgres:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT model FROM (
                            SELECT model FROM preview_index
                            UNION ALL
                            SELECT model FROM preview_assets
                        ) m
                        WHERE model ILIKE %s
                        ORDER BY model ASC
                        LIMIT %s;
                        """,
                        (f"%{partial}%", limit),
                    )
                    return [r[0] for r in cur.fetchall()]

        with self._sqlite_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT model FROM (
                    SELECT model FROM preview_index
                    UNION ALL
                    SELECT model FROM preview_assets
                )
                WHERE model LIKE ?
                ORDER BY model ASC
                LIMIT ?;
                """,
                (f"%{partial}%", limit),
            )
            return [r[0] for r in cur.fetchall()]


class PreviewIndex:
    """Robust preview index with "no repeat" per destination and auto mode.

    This class is additive and does NOT replace the legacy PreviewIndexManager.
    The main bot can use PreviewIndex for the new "random previews without
    repeating" and JobQueue-based auto previews.

    Storage:
      - SQLite file by default (works on Railway without extra config)
      - Optional Postgres if DATABASE_URL points to postgres and psycopg2 exists
        in the runtime. If not available, it automatically falls back to SQLite.
    """

    def __init__(self, database_url: str | None = None, sqlite_path: str = "previews_index.db"):
        self.database_url = (database_url or "").strip()
        self.sqlite_path = sqlite_path

        self.use_postgres = bool(self.database_url) and self.database_url.startswith(
            ("postgres://", "postgresql://")
        )
        if self.use_postgres and psycopg2 is None:
            logger.warning("Postgres URL provided but psycopg2 not available; falling back to SQLite")
            self.use_postgres = False

        # Railway: keep a persistent SQLite file in the working directory.
        try:
            self.init_db()
        except Exception as e:  # pragma: no cover
            logger.warning(f"PreviewIndex init_db failed (will continue): {e}")

    @contextmanager
    def _sqlite_conn(self):
        conn = sqlite3.connect(self.sqlite_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _pg_conn(self):
        if not psycopg2:
            raise RuntimeError("psycopg2 not available")
        conn = psycopg2.connect(self.database_url)
        try:
            yield conn
        finally:
            conn.close()

    # -----------------
    # Schema / bootstrap
    # -----------------
    def init_db(self):
        """Create required tables if they don't exist."""
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS preview_vip_messages (
                                    id SERIAL PRIMARY KEY,
                                    vip_chat_id BIGINT NOT NULL,
                                    vip_message_id BIGINT NOT NULL,
                                    model TEXT NOT NULL,
                                    media_type TEXT NOT NULL,
                                    file_unique_id TEXT NULL,
                                    caption TEXT NULL,
                                    created_at TIMESTAMP DEFAULT NOW(),
                                    UNIQUE (vip_chat_id, vip_message_id)
                                );
                                """
                            )
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS preview_sent (
                                    id SERIAL PRIMARY KEY,
                                    dest_chat_id BIGINT NOT NULL,
                                    model TEXT NOT NULL,
                                    vip_chat_id BIGINT NOT NULL,
                                    vip_message_id BIGINT NOT NULL,
                                    file_unique_id TEXT NULL,
                                    sent_at TIMESTAMP DEFAULT NOW(),
                                    UNIQUE (dest_chat_id, vip_chat_id, vip_message_id)
                                );
                                """
                            )
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS auto_previews_config (
                                    id INTEGER PRIMARY KEY,
                                    enabled INTEGER NOT NULL DEFAULT 0,
                                    interval_minutes INTEGER NOT NULL DEFAULT 1440,
                                    qty INTEGER NOT NULL DEFAULT 5,
                                    model TEXT NOT NULL DEFAULT 'all',
                                    send_to_free INTEGER NOT NULL DEFAULT 1,
                                    send_to_private INTEGER NOT NULL DEFAULT 0,
                                    free_mode TEXT NOT NULL DEFAULT 'all',
                                    free_chat_id BIGINT NULL,
                                    admin_chat_id BIGINT NULL,
                                    created_at TIMESTAMP DEFAULT NOW(),
                                    updated_at TIMESTAMP DEFAULT NOW()
                                );
                                """
                            )

                            # VIP feed module config (Coomer-based VIP seeding)
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS vip_feed_config (
                                    id INTEGER PRIMARY KEY,
                                    enabled INTEGER NOT NULL DEFAULT 0,
                                    interval_minutes INTEGER NOT NULL DEFAULT 60,
                                    page_items INTEGER NOT NULL DEFAULT 10,
                                    top_n INTEGER NOT NULL DEFAULT 100,
                                    mode TEXT NOT NULL DEFAULT 'page',
                                    head_k INTEGER NOT NULL DEFAULT 25,
                                    created_at TIMESTAMP DEFAULT NOW(),
                                    updated_at TIMESTAMP DEFAULT NOW()
                                );
                                """
                            )
                            # Backward-compatible migrations
                            cur.execute("ALTER TABLE auto_previews_config ADD COLUMN IF NOT EXISTS free_mode TEXT NOT NULL DEFAULT 'all';")
                            cur.execute("ALTER TABLE auto_previews_config ADD COLUMN IF NOT EXISTS free_chat_id BIGINT NULL;")

                            # VIP feed config migrations
                            cur.execute("ALTER TABLE vip_feed_config ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'page';")
                            cur.execute("ALTER TABLE vip_feed_config ADD COLUMN IF NOT EXISTS head_k INTEGER NOT NULL DEFAULT 25;")

                            # Global "no-repeat" across VIP + FREE for automatic modules
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS sent_media_global (
                                    media_key TEXT PRIMARY KEY,
                                    source TEXT NULL,
                                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                                );
                                """
                            )

                            # Per-destination Coomer cursor (slot engine)
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS coomer_cursor (
                                    dest_kind TEXT NOT NULL,
                                    dest_chat_id BIGINT NOT NULL,
                                    service TEXT NOT NULL,
                                    creator_id TEXT NOT NULL,
                                    creator_name TEXT NULL,
                                    offset INTEGER NOT NULL DEFAULT 0,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                    PRIMARY KEY (dest_kind, dest_chat_id)
                                );
                                """
                            )

                            # Per-destination Coomer progress (offset per creator)
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS coomer_progress (
                                    dest_kind TEXT NOT NULL,
                                    dest_chat_id BIGINT NOT NULL,
                                    service TEXT NOT NULL,
                                    creator_id TEXT NOT NULL,
                                    offset INTEGER NOT NULL DEFAULT 0,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                    PRIMARY KEY (dest_kind, dest_chat_id, service, creator_id)
                                );
                                """
                            )
                            # Per-destination Coomer state (rotation memory)
                            cur.execute(
                                """
                                CREATE TABLE IF NOT EXISTS coomer_state (
                                    dest_kind TEXT NOT NULL,
                                    dest_chat_id BIGINT NOT NULL,
                                    last_creator_key TEXT NULL,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                    PRIMARY KEY (dest_kind, dest_chat_id)
                                );
                                """
                            )

                            cur.execute(
                                """
                                INSERT INTO auto_previews_config (id) VALUES (1)
                                ON CONFLICT (id) DO NOTHING;
                                """
                            )
                            cur.execute(
                                """
                                INSERT INTO vip_feed_config (id) VALUES (1)
                                ON CONFLICT (id) DO NOTHING;
                                """
                            )
                        conn.commit()
                    return
                except Exception as e:
                    # Robustness: Postgres may not be available in the runtime.
                    # Fall back automatically to SQLite.
                    logger.warning(
                        f"PreviewIndex Postgres init failed; falling back to SQLite: {e}"
                    )
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS preview_vip_messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        vip_chat_id INTEGER NOT NULL,
                        vip_message_id INTEGER NOT NULL,
                        model TEXT NOT NULL,
                        media_type TEXT NOT NULL,
                        file_unique_id TEXT NULL,
                        caption TEXT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (vip_chat_id, vip_message_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS preview_sent (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dest_chat_id INTEGER NOT NULL,
                        model TEXT NOT NULL,
                        vip_chat_id INTEGER NOT NULL,
                        vip_message_id INTEGER NOT NULL,
                        file_unique_id TEXT NULL,
                        sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (dest_chat_id, vip_chat_id, vip_message_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS auto_previews_config (
                        id INTEGER PRIMARY KEY,
                        enabled INTEGER NOT NULL DEFAULT 0,
                        interval_minutes INTEGER NOT NULL DEFAULT 1440,
                        qty INTEGER NOT NULL DEFAULT 5,
                        model TEXT NOT NULL DEFAULT 'all',
                        send_to_free INTEGER NOT NULL DEFAULT 1,
                        send_to_private INTEGER NOT NULL DEFAULT 0,
                        free_mode TEXT NOT NULL DEFAULT 'all',
                        free_chat_id INTEGER NULL,
                        admin_chat_id INTEGER NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )

                # Global "no-repeat" across VIP + FREE for automatic modules
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sent_media_global (
                        media_key TEXT PRIMARY KEY,
                        source TEXT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )

                # Per-destination Coomer cursor (slot engine)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS coomer_cursor (
                        dest_kind TEXT NOT NULL,
                        dest_chat_id INTEGER NOT NULL,
                        service TEXT NOT NULL,
                        creator_id TEXT NOT NULL,
                        creator_name TEXT NULL,
                        offset INTEGER NOT NULL DEFAULT 0,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (dest_kind, dest_chat_id)
                    );
                    """
                )
                # VIP feed module config (Coomer-based VIP seeding)

                # Per-destination Coomer progress (offset per creator)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS coomer_progress (
                        dest_kind TEXT NOT NULL,
                        dest_chat_id INTEGER NOT NULL,
                        service TEXT NOT NULL,
                        creator_id TEXT NOT NULL,
                        offset INTEGER NOT NULL DEFAULT 0,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (dest_kind, dest_chat_id, service, creator_id)
                    );
                    """
                )
                # Per-destination Coomer state (rotation memory)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS coomer_state (
                        dest_kind TEXT NOT NULL,
                        dest_chat_id INTEGER NOT NULL,
                        last_creator_key TEXT NULL,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (dest_kind, dest_chat_id)
                    );
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS vip_feed_config (
                        id INTEGER PRIMARY KEY,
                        enabled INTEGER NOT NULL DEFAULT 0,
                        interval_minutes INTEGER NOT NULL DEFAULT 60,
                        page_items INTEGER NOT NULL DEFAULT 10,
                        top_n INTEGER NOT NULL DEFAULT 100,
                        mode TEXT NOT NULL DEFAULT 'page',
                        head_k INTEGER NOT NULL DEFAULT 25,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                cur.execute("INSERT OR IGNORE INTO vip_feed_config (id) VALUES (1);")
                # Backward-compatible migrations for existing DBs
                try:
                    cur.execute("ALTER TABLE auto_previews_config ADD COLUMN free_mode TEXT NOT NULL DEFAULT 'all';")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE auto_previews_config ADD COLUMN free_chat_id INTEGER NULL;")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE vip_feed_config ADD COLUMN mode TEXT NOT NULL DEFAULT 'page';")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE vip_feed_config ADD COLUMN head_k INTEGER NOT NULL DEFAULT 25;")
                except Exception:
                    pass
                cur.execute("INSERT OR IGNORE INTO auto_previews_config (id) VALUES (1);")
                cur.execute("INSERT OR IGNORE INTO vip_feed_config (id) VALUES (1);")
                conn.commit()
        except Exception as e:
            logger.error(f"PreviewIndex init_db error: {e}")

    # -----------------
    # VIP message indexing
    # -----------------
    def add_vip_message(
        self,
        vip_chat_id: int,
        vip_message_id: int,
        model: str,
        media_type: str,
        file_unique_id: str | None = None,
        caption: str | None = None,
    ) -> bool:
        """Upsert VIP message into the index."""
        model = (model or "all").strip().lower() or "all"
        media_type = (media_type or "unknown").strip().lower() or "unknown"
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                            """
                            INSERT INTO preview_vip_messages
                                (vip_chat_id, vip_message_id, model, media_type, file_unique_id, caption)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (vip_chat_id, vip_message_id)
                            DO UPDATE SET
                                model=EXCLUDED.model,
                                media_type=EXCLUDED.media_type,
                                file_unique_id=EXCLUDED.file_unique_id,
                                caption=EXCLUDED.caption;
                            """,
                            (int(vip_chat_id), int(vip_message_id), model, media_type, file_unique_id, caption),
                        )
                        conn.commit()
                    return True
                except Exception as e:
                    logger.warning(f"PreviewIndex Postgres add_vip_message failed; falling back to SQLite: {e}")
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO preview_vip_messages
                        (vip_chat_id, vip_message_id, model, media_type, file_unique_id, caption)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(vip_chat_id, vip_message_id)
                    DO UPDATE SET
                        model=excluded.model,
                        media_type=excluded.media_type,
                        file_unique_id=excluded.file_unique_id,
                        caption=excluded.caption;
                    """,
                    (int(vip_chat_id), int(vip_message_id), model, media_type, file_unique_id, caption),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"PreviewIndex add_vip_message error: {e}")
            return False

    def get_random_unsent(self, model: str, dest_chat_id: int, limit: int) -> list[dict]:
        """Return random VIP messages not yet sent to dest_chat_id."""
        model = (model or "all").strip().lower() or "all"
        limit = max(1, min(int(limit or 1), 50))
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            if model == "all":
                                cur.execute(
                                """
                                SELECT m.vip_chat_id, m.vip_message_id, m.model, m.media_type, m.file_unique_id, m.caption
                                FROM preview_vip_messages m
                                LEFT JOIN preview_sent s
                                    ON s.dest_chat_id=%s AND s.vip_chat_id=m.vip_chat_id AND s.vip_message_id=m.vip_message_id
                                WHERE s.id IS NULL
                                ORDER BY RANDOM()
                                LIMIT %s;
                                """,
                                (int(dest_chat_id), int(limit)),
                            )
                            else:
                                cur.execute(
                                """
                                SELECT m.vip_chat_id, m.vip_message_id, m.model, m.media_type, m.file_unique_id, m.caption
                                FROM preview_vip_messages m
                                LEFT JOIN preview_sent s
                                    ON s.dest_chat_id=%s AND s.vip_chat_id=m.vip_chat_id AND s.vip_message_id=m.vip_message_id
                                WHERE s.id IS NULL AND m.model=%s
                                ORDER BY RANDOM()
                                LIMIT %s;
                                """,
                                (int(dest_chat_id), model, int(limit)),
                            )
                            rows = cur.fetchall() or []
                            out: list[dict] = []
                            for r in rows:
                                out.append(
                                    {
                                        "vip_chat_id": int(r[0]),
                                        "vip_message_id": int(r[1]),
                                        "model": str(r[2]),
                                        "media_type": str(r[3]),
                                        "file_unique_id": r[4] if r[4] is None else str(r[4]),
                                        "caption": r[5] if r[5] is None else str(r[5]),
                                    }
                                )
                            return out
                except Exception as e:
                    logger.warning(
                        f"PreviewIndex Postgres get_random_unsent failed; falling back to SQLite: {e}"
                    )
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                if model == "all":
                    cur.execute(
                        """
                        SELECT m.vip_chat_id, m.vip_message_id, m.model, m.media_type, m.file_unique_id, m.caption
                        FROM preview_vip_messages m
                        LEFT JOIN preview_sent s
                            ON s.dest_chat_id=? AND s.vip_chat_id=m.vip_chat_id AND s.vip_message_id=m.vip_message_id
                        WHERE s.id IS NULL
                        ORDER BY RANDOM()
                        LIMIT ?;
                        """,
                        (int(dest_chat_id), int(limit)),
                    )
                else:
                    cur.execute(
                        """
                        SELECT m.vip_chat_id, m.vip_message_id, m.model, m.media_type, m.file_unique_id, m.caption
                        FROM preview_vip_messages m
                        LEFT JOIN preview_sent s
                            ON s.dest_chat_id=? AND s.vip_chat_id=m.vip_chat_id AND s.vip_message_id=m.vip_message_id
                        WHERE s.id IS NULL AND m.model=?
                        ORDER BY RANDOM()
                        LIMIT ?;
                        """,
                        (int(dest_chat_id), model, int(limit)),
                    )
                rows = cur.fetchall() or []
                return [
                    {
                        "vip_chat_id": int(r["vip_chat_id"]),
                        "vip_message_id": int(r["vip_message_id"]),
                        "model": str(r["model"]),
                        "media_type": str(r["media_type"]),
                        "file_unique_id": r["file_unique_id"],
                        "caption": r["caption"],
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.error(f"PreviewIndex get_random_unsent error: {e}")
            return []

    def mark_sent(
        self,
        dest_chat_id: int,
        model: str,
        vip_chat_id: int,
        vip_message_id: int,
        file_unique_id: str | None = None,
    ) -> bool:
        model = (model or "all").strip().lower() or "all"
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                            """
                            INSERT INTO preview_sent
                                (dest_chat_id, model, vip_chat_id, vip_message_id, file_unique_id)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (dest_chat_id, vip_chat_id, vip_message_id) DO NOTHING;
                            """,
                            (int(dest_chat_id), model, int(vip_chat_id), int(vip_message_id), file_unique_id),
                        )
                        conn.commit()
                    return True
                except Exception as e:
                    logger.warning(
                        f"PreviewIndex Postgres mark_sent failed; falling back to SQLite: {e}"
                    )
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT OR IGNORE INTO preview_sent
                        (dest_chat_id, model, vip_chat_id, vip_message_id, file_unique_id)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (int(dest_chat_id), model, int(vip_chat_id), int(vip_message_id), file_unique_id),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"PreviewIndex mark_sent error: {e}")
            return False

    # -----------------
    # Auto previews config
    # -----------------
    def set_auto_config(
        self,
        enabled: int,
        interval_minutes: int,
        qty: int,
        model: str,
        send_to_free: int,
        send_to_private: int,
        admin_chat_id: int | None,
        free_mode: str = "all",
        free_chat_id: int | None = None,
    ) -> bool:
        model = (model or "all").strip().lower() or "all"
        interval_minutes = max(1, int(interval_minutes or 1440))
        qty = max(1, min(int(qty or 5), 50))
        free_mode = (free_mode or "all").strip().lower() or "all"
        if free_mode not in ("all", "single"):
            free_mode = "all"
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                            """
                            INSERT INTO auto_previews_config
                                (id, enabled, interval_minutes, qty, model, send_to_free, send_to_private, free_mode, free_chat_id, admin_chat_id, updated_at)
                            VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (id)
                            DO UPDATE SET
                                enabled=EXCLUDED.enabled,
                                interval_minutes=EXCLUDED.interval_minutes,
                                qty=EXCLUDED.qty,
                                model=EXCLUDED.model,
                                send_to_free=EXCLUDED.send_to_free,
                                send_to_private=EXCLUDED.send_to_private,
                                free_mode=EXCLUDED.free_mode,
                                free_chat_id=EXCLUDED.free_chat_id,
                                admin_chat_id=EXCLUDED.admin_chat_id,
                                updated_at=NOW();
                            """,
                            (
                                int(enabled),
                                int(interval_minutes),
                                int(qty),
                                model,
                                int(send_to_free),
                                int(send_to_private),
                                free_mode,
                                free_chat_id if free_chat_id else None,
                                admin_chat_id if admin_chat_id else None,
                            ),
                        )
                        conn.commit()
                    return True
                except Exception as e:
                    logger.warning(
                        f"PreviewIndex Postgres set_auto_config failed; falling back to SQLite: {e}"
                    )
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO auto_previews_config
                        (id, enabled, interval_minutes, qty, model, send_to_free, send_to_private, free_mode, free_chat_id, admin_chat_id, updated_at)
                    VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        enabled=excluded.enabled,
                        interval_minutes=excluded.interval_minutes,
                        qty=excluded.qty,
                        model=excluded.model,
                        send_to_free=excluded.send_to_free,
                        send_to_private=excluded.send_to_private,
                        free_mode=excluded.free_mode,
                        free_chat_id=excluded.free_chat_id,
                        admin_chat_id=excluded.admin_chat_id,
                        updated_at=CURRENT_TIMESTAMP;
                    """,
                    (
                        int(enabled),
                        int(interval_minutes),
                        int(qty),
                        model,
                        int(send_to_free),
                        int(send_to_private),
                        free_mode,
                        int(free_chat_id) if free_chat_id else None,
                        int(admin_chat_id) if admin_chat_id else None,
                    ),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"PreviewIndex set_auto_config error: {e}")
            return False


    def has_sent(self, dest_chat_id: int, model: str, vip_chat_id: int, vip_message_id: int) -> bool:
        """Return True if this (dest_chat_id, vip_chat_id, vip_message_id) was already sent.

        Used by Coomer-based auto previews to avoid repeating the same media.
        """
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT 1 FROM preview_sent WHERE dest_chat_id=%s AND vip_chat_id=%s AND vip_message_id=%s LIMIT 1",
                        (int(dest_chat_id), int(vip_chat_id), int(vip_message_id)),
                    )
                    row = cur.fetchone()
                    return bool(row)
            else:
                with self._sqlite_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT 1 FROM preview_sent WHERE dest_chat_id=? AND vip_chat_id=? AND vip_message_id=? LIMIT 1",
                        (int(dest_chat_id), int(vip_chat_id), int(vip_message_id)),
                    )
                    row = cur.fetchone()
                    return bool(row)
        except Exception:
            return False

    # -----------------
    # Global no-repeat (automatic modules)
    # -----------------
    def global_has_sent(self, media_key: str) -> bool:
        media_key = (media_key or "").strip()
        if not media_key:
            return False
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM sent_media_global WHERE media_key=%s LIMIT 1;", (media_key,))
                        return bool(cur.fetchone())
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM sent_media_global WHERE media_key=? LIMIT 1;", (media_key,))
                return bool(cur.fetchone())
        except Exception:
            return False

    def global_mark_sent(self, media_key: str, source: str | None = None) -> bool:
        media_key = (media_key or "").strip()
        if not media_key:
            return False
        source = (source or "").strip() or None
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO sent_media_global(media_key, source) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                            (media_key, source),
                        )
                    conn.commit()
                    return True
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT OR IGNORE INTO sent_media_global(media_key, source) VALUES (?, ?);",
                    (media_key, source),
                )
                conn.commit()
                return True
        except Exception:
            return False

    # -----------------
    # Coomer cursor (slot engine)
    # -----------------
    def get_coomer_cursor(self, dest_kind: str, dest_chat_id: int) -> dict | None:
        dest_kind = (dest_kind or "").strip().lower()
        if not dest_kind:
            return None
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT service, creator_id, creator_name, offset FROM coomer_cursor WHERE dest_kind=%s AND dest_chat_id=%s LIMIT 1;",
                            (dest_kind, int(dest_chat_id)),
                        )
                        row = cur.fetchone()
                        if not row:
                            return None
                        return {"service": row[0], "creator_id": row[1], "creator_name": row[2], "offset": int(row[3] or 0)}
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT service, creator_id, creator_name, offset FROM coomer_cursor WHERE dest_kind=? AND dest_chat_id=? LIMIT 1;",
                    (dest_kind, int(dest_chat_id)),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {"service": row[0], "creator_id": row[1], "creator_name": row[2], "offset": int(row[3] or 0)}
        except Exception:
            return None

    def set_coomer_cursor(self, dest_kind: str, dest_chat_id: int, service: str, creator_id: str, creator_name: str | None, offset: int) -> bool:
        dest_kind = (dest_kind or "").strip().lower()
        service = (service or "").strip().lower()
        creator_id = (creator_id or "").strip()
        creator_name = (creator_name or "").strip() or None
        offset = int(offset or 0)
        if not dest_kind or not service or not creator_id:
            return False
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO coomer_cursor(dest_kind, dest_chat_id, service, creator_id, creator_name, offset, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, now())
                            ON CONFLICT (dest_kind, dest_chat_id)
                            DO UPDATE SET service=EXCLUDED.service, creator_id=EXCLUDED.creator_id, creator_name=EXCLUDED.creator_name, offset=EXCLUDED.offset, updated_at=now();
                            """,
                            (dest_kind, int(dest_chat_id), service, creator_id, creator_name, int(offset)),
                        )
                    conn.commit()
                    return True
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO coomer_cursor(dest_kind, dest_chat_id, service, creator_id, creator_name, offset, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(dest_kind, dest_chat_id)
                    DO UPDATE SET service=excluded.service, creator_id=excluded.creator_id, creator_name=excluded.creator_name, offset=excluded.offset, updated_at=CURRENT_TIMESTAMP;
                    """,
                    (dest_kind, int(dest_chat_id), service, creator_id, creator_name, int(offset)),
                )
                conn.commit()
                return True
        except Exception:
            return False

    # --- Coomer state/progress (rotation + per-creator offsets) ---

    def get_coomer_progress(self, dest_kind: str, dest_chat_id: int, service: str, creator_id: str) -> int:
        dest_kind = (dest_kind or "").strip().lower()
        service = (service or "").strip().lower()
        creator_id = (creator_id or "").strip()
        if not dest_kind or not service or not creator_id:
            return 0
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT offset FROM coomer_progress WHERE dest_kind=%s AND dest_chat_id=%s AND service=%s AND creator_id=%s",
                            (dest_kind, int(dest_chat_id), service, creator_id),
                        )
                        row = cur.fetchone()
                        return int(row[0]) if row else 0
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT offset FROM coomer_progress WHERE dest_kind=? AND dest_chat_id=? AND service=? AND creator_id=?",
                    (dest_kind, int(dest_chat_id), service, creator_id),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
        except Exception:
            return 0

    def set_coomer_progress(self, dest_kind: str, dest_chat_id: int, service: str, creator_id: str, offset: int) -> bool:
        dest_kind = (dest_kind or "").strip().lower()
        service = (service or "").strip().lower()
        creator_id = (creator_id or "").strip()
        offset = int(offset or 0)
        if not dest_kind or not service or not creator_id:
            return False
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO coomer_progress(dest_kind, dest_chat_id, service, creator_id, offset, updated_at)
                            VALUES (%s, %s, %s, %s, %s, now())
                            ON CONFLICT (dest_kind, dest_chat_id, service, creator_id)
                            DO UPDATE SET offset=EXCLUDED.offset, updated_at=now();
                            """,
                            (dest_kind, int(dest_chat_id), service, creator_id, offset),
                        )
                conn.commit()
                return True
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO coomer_progress(dest_kind, dest_chat_id, service, creator_id, offset, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(dest_kind, dest_chat_id, service, creator_id)
                    DO UPDATE SET offset=excluded.offset, updated_at=CURRENT_TIMESTAMP;
                    """,
                    (dest_kind, int(dest_chat_id), service, creator_id, offset),
                )
                conn.commit()
                return True
        except Exception:
            return False

    def get_coomer_state(self, dest_kind: str, dest_chat_id: int) -> str | None:
        dest_kind = (dest_kind or "").strip().lower()
        if not dest_kind:
            return None
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT last_creator_key FROM coomer_state WHERE dest_kind=%s AND dest_chat_id=%s",
                            (dest_kind, int(dest_chat_id)),
                        )
                        row = cur.fetchone()
                        return str(row[0]) if row and row[0] is not None else None
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT last_creator_key FROM coomer_state WHERE dest_kind=? AND dest_chat_id=?",
                    (dest_kind, int(dest_chat_id)),
                )
                row = cur.fetchone()
                return str(row[0]) if row and row[0] is not None else None
        except Exception:
            return None

    def set_coomer_state(self, dest_kind: str, dest_chat_id: int, last_creator_key: str | None) -> bool:
        dest_kind = (dest_kind or "").strip().lower()
        if not dest_kind:
            return False
        last_creator_key = (last_creator_key or "").strip() or None
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO coomer_state(dest_kind, dest_chat_id, last_creator_key, updated_at)
                            VALUES (%s, %s, %s, now())
                            ON CONFLICT (dest_kind, dest_chat_id)
                            DO UPDATE SET last_creator_key=EXCLUDED.last_creator_key, updated_at=now();
                            """,
                            (dest_kind, int(dest_chat_id), last_creator_key),
                        )
                conn.commit()
                return True
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO coomer_state(dest_kind, dest_chat_id, last_creator_key, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(dest_kind, dest_chat_id)
                    DO UPDATE SET last_creator_key=excluded.last_creator_key, updated_at=CURRENT_TIMESTAMP;
                    """,
                    (dest_kind, int(dest_chat_id), last_creator_key),
                )
                conn.commit()
                return True
        except Exception:
            return False


    def clear_coomer_cursor(self, dest_kind: str, dest_chat_id: int) -> bool:
        dest_kind = (dest_kind or "").strip().lower()
        if not dest_kind:
            return False
        try:
            if self.use_postgres:
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM coomer_cursor WHERE dest_kind=%s AND dest_chat_id=%s;", (dest_kind, int(dest_chat_id)))
                    conn.commit()
                    return True
            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM coomer_cursor WHERE dest_kind=? AND dest_chat_id=?;", (dest_kind, int(dest_chat_id)))
                conn.commit()
                return True
        except Exception:
            return False

    def get_auto_config(self) -> dict:
        """Return the current auto previews config (defaults if missing)."""
        defaults = {
            'enabled': 0,
            'interval_minutes': 1440,
            'qty': 5,
            'model': 'all',
            'send_to_free': 1,
            'send_to_private': 0,
            'free_mode': 'all',
            'free_chat_id': None,
            'admin_chat_id': None,
        }
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT enabled, interval_minutes, qty, model, send_to_free, send_to_private, free_mode, free_chat_id, admin_chat_id
                                FROM auto_previews_config
                                WHERE id=1;
                                """
                            )
                            row = cur.fetchone()
                            if not row:
                                return dict(defaults)
                            return {
                                'enabled': int(row[0] or 0),
                                'interval_minutes': int(row[1] or defaults['interval_minutes']),
                                'qty': int(row[2] or defaults['qty']),
                                'model': str(row[3] or defaults['model']),
                                'send_to_free': int(row[4] if row[4] is not None else defaults['send_to_free']),
                                'send_to_private': int(row[5] if row[5] is not None else defaults['send_to_private']),
                                'free_mode': str(row[6] or defaults['free_mode']),
                                'free_chat_id': None if row[7] is None else int(row[7]),
                                'admin_chat_id': None if row[8] is None else int(row[8]),
                            }
                except Exception as e:
                    logger.warning(f"PreviewIndex Postgres get_auto_config failed; falling back to SQLite: {e}")
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT enabled, interval_minutes, qty, model, send_to_free, send_to_private, free_mode, free_chat_id, admin_chat_id
                    FROM auto_previews_config
                    WHERE id=1;
                    """
                )
                row = cur.fetchone()
                if not row:
                    return dict(defaults)
                # sqlite3.Row can occasionally raise "No item with that key" even
                # when keys() claims the key exists (observed on some deploys).
                # Build a stable dict using numeric indices.
                try:
                    row_dict = {k: row[i] for i, k in enumerate(row.keys())}
                except Exception:
                    row_dict = {}
                keys = set(row_dict.keys())
                def _get(key, default=None):
                    return row_dict.get(key, default) if key in keys else default
                return {
                    'enabled': int(_get('enabled', defaults['enabled']) or 0),
                    'interval_minutes': int(_get('interval_minutes', defaults['interval_minutes']) or defaults['interval_minutes']),
                    'qty': int(_get('qty', defaults['qty']) or defaults['qty']),
                    'model': str(_get('model', defaults['model']) or defaults['model']),
                    'send_to_free': int(_get('send_to_free', defaults['send_to_free']) if _get('send_to_free', None) is not None else defaults['send_to_free']),
                    'send_to_private': int(_get('send_to_private', defaults['send_to_private']) if _get('send_to_private', None) is not None else defaults['send_to_private']),
                    'free_mode': str(_get('free_mode', defaults['free_mode']) or defaults['free_mode']),
                    'free_chat_id': None if _get('free_chat_id', None) is None else int(_get('free_chat_id')),
                    'admin_chat_id': None if _get('admin_chat_id', None) is None else int(_get('admin_chat_id')),
                }
        except Exception as e:
            logger.error(f"PreviewIndex get_auto_config error: {e}")
            return dict(defaults)


    # -----------------
    # VIP feed module config
    # -----------------
    # -----------------
    # VIP feed module config
    # -----------------
    # -----------------
    # VIP feed module config
    # -----------------

    # -----------------
    # VIP feed module config
    # -----------------
    # -----------------
    # VIP feed module config
    # -----------------
    def set_vip_feed_config(
        self,
        enabled: int,
        interval_minutes: int,
        page_items: int,
        top_n: int = 100,
        mode: str = "page",
        head_k: int = 25,
    ) -> bool:
        """Persist VIP feed configuration."""
        interval_minutes = max(1, int(interval_minutes or 60))
        page_items = max(1, min(int(page_items or 10), 50))
        top_n = max(10, min(int(top_n or 100), 500))
        mode = (mode or "page").strip().lower()
        if mode not in ("page", "all"):
            mode = "page"
        head_k = max(5, min(int(head_k or 25), 100))

        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO vip_feed_config
                                    (id, enabled, interval_minutes, page_items, top_n, mode, head_k, updated_at)
                                VALUES (1, %s, %s, %s, %s, %s, %s, NOW())
                                ON CONFLICT (id)
                                DO UPDATE SET
                                    enabled=EXCLUDED.enabled,
                                    interval_minutes=EXCLUDED.interval_minutes,
                                    page_items=EXCLUDED.page_items,
                                    top_n=EXCLUDED.top_n,
                                    mode=EXCLUDED.mode,
                                    head_k=EXCLUDED.head_k,
                                    updated_at=NOW();
                                """,
                                (int(enabled), int(interval_minutes), int(page_items), int(top_n), str(mode), int(head_k)),
                            )
                        conn.commit()
                    return True
                except Exception as e:
                    logger.warning(
                        f"PreviewIndex Postgres set_vip_feed_config failed; falling back to SQLite: {e}"
                    )
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO vip_feed_config
                        (id, enabled, interval_minutes, page_items, top_n, mode, head_k, updated_at)
                    VALUES (1, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        enabled=excluded.enabled,
                        interval_minutes=excluded.interval_minutes,
                        page_items=excluded.page_items,
                        top_n=excluded.top_n,
                        mode=excluded.mode,
                        head_k=excluded.head_k,
                        updated_at=CURRENT_TIMESTAMP;
                    """,
                    (int(enabled), int(interval_minutes), int(page_items), int(top_n), str(mode), int(head_k)),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"PreviewIndex set_vip_feed_config error: {e}")
            return False


    def get_vip_feed_config(self) -> dict:
        defaults = {
            "enabled": 0,
            "interval_minutes": 60,
            "page_items": 10,
            "top_n": 100,
            "mode": "page",
            "head_k": 25,
        }
        try:
            if self.use_postgres:
                try:
                    with self._pg_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT enabled, interval_minutes, page_items, top_n, mode, head_k
                                FROM vip_feed_config WHERE id=1;
                                """
                            )
                            row = cur.fetchone()
                            if not row:
                                return dict(defaults)
                            return {
                                "enabled": int(row[0]),
                                "interval_minutes": int(row[1]),
                                "page_items": int(row[2]),
                                "top_n": int(row[3]),
                                "mode": str(row[4] or "page"),
                                "head_k": int(row[5] or 25),
                            }
                except Exception as e:
                    logger.warning(
                        f"PreviewIndex Postgres get_vip_feed_config failed; falling back to SQLite: {e}"
                    )
                    self.use_postgres = False

            with self._sqlite_conn() as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT enabled, interval_minutes, page_items, top_n, mode, head_k
                    FROM vip_feed_config WHERE id=1;
                    """
                )
                row = cur.fetchone()
                if not row:
                    return dict(defaults)
                keys = set(row.keys())
                return {
                    "enabled": int(row["enabled"]),
                    "interval_minutes": int(row["interval_minutes"]),
                    "page_items": int(row["page_items"]),
                    "top_n": int(row["top_n"]),
                    "mode": str(row["mode"]) if ("mode" in keys and row["mode"] is not None) else "page",
                    "head_k": int(row["head_k"] or 25) if "head_k" in keys else 25,
                }
        except Exception as e:
            logger.error(f"PreviewIndex get_vip_feed_config error: {e}")
            return dict(defaults)
