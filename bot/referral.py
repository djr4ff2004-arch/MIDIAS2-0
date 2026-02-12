"""Referral system for Telegram VIP Media Bot.

Lightweight referral tracker:
- Users obtain a personal deep-link via /ref.
- When a new user starts the bot with /start ref_<id>, a referral is recorded.
- Admin configures milestone goals; when reached, user + admin are notified.

No automatic VIP granting is performed (admin can grant VIP manually elsewhere).

Storage: SQLite file (no extra dependencies).
"""

from __future__ import annotations

import logging
import sqlite3
import os

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None

from pathlib import Path
from typing import Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ReferralManager:
    def __init__(self, db_path: str = "referrals.db"):
        self.database_url = os.getenv('DATABASE_URL', '')
        self.use_postgres = (psycopg2 is not None) and (self.database_url.lower().startswith('postgres://') or self.database_url.lower().startswith('postgresql://'))
        self.db_path = Path(db_path)
        self._init_db()

    
    def _pg_connect(self):
        if not self.use_postgres:
            return None
        try:
            return psycopg2.connect(self.database_url)
        except Exception as e:
            logger.error(f"Postgres connect failed (referrals): {e}")
            return None

    def _ensure_pg_schema(self):
        conn = self._pg_connect()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """CREATE TABLE IF NOT EXISTS referrals (
                        referred_user_id BIGINT PRIMARY KEY,
                        referrer_user_id BIGINT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );"""
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_referrer_user_id ON referrals(referrer_user_id);")
            conn.commit()
        except Exception as e:
            logger.error(f"Postgres schema init failed (referrals): {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path.as_posix(), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS referrals (
                    referred_user_id INTEGER PRIMARY KEY,
                    referrer_user_id INTEGER NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS milestones (
                    user_id INTEGER NOT NULL,
                    goal INTEGER NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (user_id, goal)
                )
                """
            )

    def record_referral(self, referrer_id: int, referred_id: int) -> bool:
        if self.use_postgres:
            return self._record_referral_pg(referrer_id, referred_id)
        """Record a referral once per referred user.

        Returns True if recorded, False if already exists or invalid.
        """
        if not referrer_id or not referred_id:
            return False
        if referrer_id == referred_id:
            return False

        try:
            with self._conn() as conn:
                # If referred_id already exists, ignore.
                cur = conn.execute(
                    "SELECT 1 FROM referrals WHERE referred_user_id=?", (referred_id,)
                )
                if cur.fetchone():
                    return False
                conn.execute(
                    "INSERT INTO referrals(referred_user_id, referrer_user_id) VALUES (?, ?)",
                    (referred_id, referrer_id),
                )
            return True
        except Exception as e:
            logger.error(f"Failed to record referral {referrer_id}->{referred_id}: {e}")
            return False

    def get_referral_count(self, user_id: int) -> int:
        if self.use_postgres:
            return self._get_referral_count_pg(user_id)
        try:
            with self._conn() as conn:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM referrals WHERE referrer_user_id=?", (user_id,)
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
        except Exception as e:
            logger.error(f"Failed to get referral count for {user_id}: {e}")
            return 0

    def get_top_referrers(self, limit: int = 10) -> List[Tuple[int, int]]:
        try:
            with self._conn() as conn:
                cur = conn.execute(
                    """
                    SELECT referrer_user_id, COUNT(*) as c
                    FROM referrals
                    GROUP BY referrer_user_id
                    ORDER BY c DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                return [(int(uid), int(c)) for uid, c in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get top referrers: {e}")
            return []

    def milestone_already_sent(self, user_id: int, goal: int) -> bool:
        try:
            with self._conn() as conn:
                cur = conn.execute(
                    "SELECT 1 FROM milestones WHERE user_id=? AND goal=?", (user_id, goal)
                )
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed milestone lookup {user_id}@{goal}: {e}")
            return False

    def mark_milestone_sent(self, user_id: int, goal: int) -> None:
        try:
            with self._conn() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO milestones(user_id, goal) VALUES (?, ?)",
                    (user_id, goal),
                )
        except Exception as e:
            logger.error(f"Failed to mark milestone {user_id}@{goal}: {e}")

    @staticmethod
    def parse_goals(raw: str) -> List[int]:
        """Parse a goals string like '5,10,20' or '5 10 20' into sorted unique ints."""
        if not raw:
            return []
        parts = [p.strip() for p in raw.replace(";", ",").replace(" ", ",").split(",")]
        goals = []
        for p in parts:
            if not p:
                continue
            try:
                v = int(p)
                if v > 0:
                    goals.append(v)
            except ValueError:
                continue
        return sorted(set(goals))
