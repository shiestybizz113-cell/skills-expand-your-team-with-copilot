from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Literal

TaskStatus = Literal["queued", "claimed", "blocked", "completed", "failed"]
RiskLevel = Literal["safe", "approval_required", "forbidden"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Task:
    id: str
    title: str
    universe: str
    repo: str
    action: str
    priority: int
    risk: RiskLevel
    status: TaskStatus
    worker: str | None
    created_at: str
    updated_at: str
    evidence: list[dict[str, Any]]


class OperatorStore:
    """SQLite-backed task queue with atomic claims and append-only evidence."""

    def __init__(self, path: str | Path = "empire_operator.db") -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    universe TEXT NOT NULL,
                    repo TEXT NOT NULL,
                    action TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 50,
                    risk TEXT NOT NULL CHECK(risk IN ('safe','approval_required','forbidden')),
                    status TEXT NOT NULL CHECK(status IN ('queued','claimed','blocked','completed','failed')),
                    worker TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evidence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL REFERENCES tasks(id),
                    kind TEXT NOT NULL,
                    label TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_queue
                    ON tasks(status, priority DESC, created_at ASC);
                """
            )

    def add_task(
        self,
        *,
        title: str,
        universe: str,
        repo: str,
        action: str,
        priority: int = 50,
        risk: RiskLevel = "safe",
    ) -> Task:
        if risk == "forbidden":
            status: TaskStatus = "blocked"
        else:
            status = "queued"
        task_id = str(uuid.uuid4())
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO tasks
                (id,title,universe,repo,action,priority,risk,status,worker,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (task_id, title, universe, repo, action, priority, risk, status, None, now, now),
            )
        return self.get_task(task_id)

    def claim_next(self, worker: str) -> Task | None:
        """Atomically claim one safe task. Approval-required work never auto-runs."""
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT id FROM tasks
                WHERE status='queued' AND risk='safe'
                ORDER BY priority DESC, created_at ASC LIMIT 1"""
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            now = utc_now()
            changed = conn.execute(
                """UPDATE tasks SET status='claimed', worker=?, updated_at=?
                WHERE id=? AND status='queued'""",
                (worker, now, row["id"]),
            ).rowcount
            conn.execute("COMMIT")
            return self.get_task(row["id"]) if changed == 1 else None

    def approve(self, task_id: str) -> Task:
        with self._connect() as conn:
            conn.execute(
                """UPDATE tasks SET risk='safe', status='queued', updated_at=?
                WHERE id=? AND risk='approval_required' AND status IN ('queued','blocked')""",
                (utc_now(), task_id),
            )
        return self.get_task(task_id)

    def finish(
        self,
        task_id: str,
        *,
        success: bool,
        kind: str,
        label: str,
        payload: dict[str, Any],
    ) -> Task:
        status: TaskStatus = "completed" if success else "failed"
        now = utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO evidence(task_id,kind,label,payload,created_at) VALUES (?,?,?,?,?)",
                (task_id, kind, label, json.dumps(payload, sort_keys=True), now),
            )
            conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE id=? AND status='claimed'",
                (status, now, task_id),
            )
            conn.execute("COMMIT")
        return self.get_task(task_id)

    def list_tasks(self) -> list[Task]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks ORDER BY priority DESC, created_at ASC"
            ).fetchall()
        return [self._hydrate(row) for row in rows]

    def get_task(self, task_id: str) -> Task:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        return self._hydrate(row)

    def summary(self) -> dict[str, int]:
        counts = {key: 0 for key in ("queued", "claimed", "blocked", "completed", "failed")}
        with self._connect() as conn:
            for row in conn.execute("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"):
                counts[row["status"]] = row["n"]
        return counts

    def _hydrate(self, row: sqlite3.Row) -> Task:
        with self._connect() as conn:
            evidence_rows = conn.execute(
                "SELECT kind,label,payload,created_at FROM evidence WHERE task_id=? ORDER BY id",
                (row["id"],),
            ).fetchall()
        evidence = [
            {
                "kind": item["kind"],
                "label": item["label"],
                "payload": json.loads(item["payload"]),
                "created_at": item["created_at"],
            }
            for item in evidence_rows
        ]
        return Task(
            id=row["id"], title=row["title"], universe=row["universe"], repo=row["repo"],
            action=row["action"], priority=row["priority"], risk=row["risk"],
            status=row["status"], worker=row["worker"], created_at=row["created_at"],
            updated_at=row["updated_at"], evidence=evidence,
        )


def task_dict(task: Task) -> dict[str, Any]:
    return asdict(task)
