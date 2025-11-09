#!/usr/bin/env python3
"""
storage.py – SQLite-based job storage for QueueCTL (Windows-safe)
Each DB call opens and closes its own connection, so it works with multiprocessing spawn mode.
"""

import sqlite3
import os
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

DB_PATH = os.path.join(os.getcwd(), "queuectl.db")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._ensure_db()

    # ------------------------------------------------------------------ #
    def _conn(self):
        """Return a *new* open SQLite connection each call."""
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------ #
    def _ensure_db(self):
        """Create DB and tables if they don’t exist."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            max_retries INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_error TEXT
        )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_state ON jobs(state)")
        c.execute("PRAGMA journal_mode=WAL;")  # allow concurrent readers/writers
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------ #
    def add_job(self, job: Dict[str, Any]) -> None:
        conn = self._conn()
        now = _now_iso()
        try:
            conn.execute("""
                INSERT INTO jobs (id, command, state, attempts, max_retries,
                                  created_at, updated_at, last_error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job["id"],
                job["command"],
                job.get("state", "pending"),
                job.get("attempts", 0),
                job.get("max_retries", 3),
                job.get("created_at", now),
                job.get("updated_at", now),
                job.get("last_error"),
            ))
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def list_jobs(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self._conn()
        try:
            if state:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE state = ? ORDER BY created_at", (state,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at"
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def summary(self) -> Dict[str, int]:
        conn = self._conn()
        try:
            cur = conn.execute("SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state").fetchall()
            return {r["state"]: r["cnt"] for r in cur}
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def move_to_state(self, job_id: str, new_state: str, last_error: Optional[str] = None):
        conn = self._conn()
        now = _now_iso()
        try:
            conn.execute(
                "UPDATE jobs SET state=?, updated_at=?, last_error=? WHERE id=?",
                (new_state, now, last_error, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def increment_attempts_and_lock(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Increment attempts and mark job as processing if it’s pending/failed."""
        conn = self._conn()
        now = _now_iso()
        try:
            res = conn.execute("""
                UPDATE jobs
                SET attempts = attempts + 1, state = 'processing', updated_at = ?
                WHERE id = ? AND state IN ('pending','failed')
            """, (now, job_id))
            if res.rowcount == 0:
                return None
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            conn.commit()
            return dict(row)
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def fetch_and_lock_next_job(self) -> Optional[Dict[str, Any]]:
        """Return one job ready for processing (pending/failed)."""
        conn = self._conn()
        try:
            row = conn.execute("""
                SELECT * FROM jobs
                WHERE state IN ('pending','failed')
                ORDER BY created_at LIMIT 1
            """).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    def update_job_after_execution(
        self, job_id: str, success: bool, attempts: int, max_retries: int, err: Optional[str] = None
    ):
        conn = self._conn()
        now = _now_iso()
        try:
            if success:
                conn.execute(
                    "UPDATE jobs SET state='completed', updated_at=?, last_error=NULL WHERE id=?",
                    (now, job_id),
                )
            else:
                if attempts >= max_retries:
                    conn.execute(
                        "UPDATE jobs SET state='dead', updated_at=?, last_error=? WHERE id=?",
                        (now, err, job_id),
                    )
                else:
                    conn.execute(
                        "UPDATE jobs SET state='failed', updated_at=?, last_error=? WHERE id=?",
                        (now, err, job_id),
                    )
            conn.commit()
        finally:
            conn.close()
