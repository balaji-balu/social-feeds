from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence


@dataclass(frozen=True)
class Post:
    source: str
    source_id: str
    title: str
    url: str
    text: str = ""
    published_at: str | None = None


@dataclass(frozen=True)
class Theme:
    theme: str
    summary: str
    example_posts: Sequence[str]


@dataclass(frozen=True)
class RunSummary:
    discovered_posts: int
    candidates: int
    completed_batches: int
    failed_batches: int


@dataclass(frozen=True)
class SourceBatch:
    posts: Sequence[Post]
    cursor: int | str | None = None


class SourceClient(Protocol):
    async def fetch(self) -> Sequence[Post] | SourceBatch: ...


class LlmClient(Protocol):
    async def analyze(self, posts: Sequence[Post]) -> Sequence[Theme]: ...


class _Store:
    def __init__(self, database_path: Path):
        self.connection = sqlite3.connect(database_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS posts (
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                text TEXT NOT NULL,
                published_at TEXT,
                status TEXT NOT NULL CHECK (status IN ('discovered', 'candidate', 'batched', 'completed')),
                PRIMARY KEY (source, source_id)
            );
            CREATE TABLE IF NOT EXISTS source_cursors (
                source_key TEXT PRIMARY KEY,
                watermark INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed_retryable'))
            );
            CREATE TABLE IF NOT EXISTS batch_posts (
                batch_id INTEGER NOT NULL REFERENCES batches(id),
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                PRIMARY KEY (batch_id, source, source_id),
                FOREIGN KEY (source, source_id) REFERENCES posts(source, source_id)
            );
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL REFERENCES batches(id),
                status TEXT NOT NULL CHECK (status IN ('processing', 'succeeded', 'failed')),
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS themes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL REFERENCES batches(id),
                theme TEXT NOT NULL,
                summary TEXT NOT NULL,
                example_posts TEXT NOT NULL
            );
            """
        )
        self.connection.commit()

    def add_post(self, post: Post) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO posts
                (source, source_id, title, url, text, published_at, status)
            VALUES (?, ?, ?, ?, ?, ?, 'discovered')
            """,
            (post.source, post.source_id, post.title, post.url, post.text, post.published_at),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def mark_candidate(self, post: Post) -> None:
        self.connection.execute(
            "UPDATE posts SET status = 'candidate' WHERE source = ? AND source_id = ?",
            (post.source, post.source_id),
        )
        self.connection.commit()

    def create_batch(self, posts: Sequence[Post]) -> tuple[int, int]:
        with self.connection:
            cursor = self.connection.execute("INSERT INTO batches(status) VALUES ('processing')")
            batch_id = cursor.lastrowid
            self.connection.executemany(
                "INSERT INTO batch_posts(batch_id, source, source_id) VALUES (?, ?, ?)",
                [(batch_id, post.source, post.source_id) for post in posts],
            )
            self.connection.executemany(
                "UPDATE posts SET status = 'batched' WHERE source = ? AND source_id = ?",
                [(post.source, post.source_id) for post in posts],
            )
            attempt = self.connection.execute(
                "INSERT INTO attempts(batch_id, status) VALUES (?, 'processing')", (batch_id,)
            )
        return int(batch_id), int(attempt.lastrowid)

    def complete_batch(self, batch_id: int, attempt_id: int, themes: Sequence[Theme]) -> None:
        with self.connection:
            self.connection.executemany(
                "INSERT INTO themes(batch_id, theme, summary, example_posts) VALUES (?, ?, ?, ?)",
                [
                    (batch_id, theme.theme, theme.summary, json.dumps(list(theme.example_posts)))
                    for theme in themes
                ],
            )
            self.connection.execute("UPDATE attempts SET status = 'succeeded' WHERE id = ?", (attempt_id,))
            self.connection.execute("UPDATE batches SET status = 'completed' WHERE id = ?", (batch_id,))
            self.connection.execute(
                """
                UPDATE posts SET status = 'completed'
                WHERE (source, source_id) IN (
                    SELECT source, source_id FROM batch_posts WHERE batch_id = ?
                )
                """,
                (batch_id,),
            )

    def fail_batch(self, batch_id: int, attempt_id: int, error: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE attempts SET status = 'failed', error = ? WHERE id = ?", (error, attempt_id)
            )
            self.connection.execute("UPDATE batches SET status = 'failed_retryable' WHERE id = ?", (batch_id,))
            self.connection.execute(
                """
                UPDATE posts SET status = 'candidate'
                WHERE (source, source_id) IN (
                    SELECT source, source_id FROM batch_posts WHERE batch_id = ?
                )
                """,
                (batch_id,),
            )

    def list_themes(self) -> list[Theme]:
        rows = self.connection.execute(
            "SELECT theme, summary, example_posts FROM themes ORDER BY id"
        ).fetchall()
        return [
            Theme(row["theme"], row["summary"], json.loads(row["example_posts"])) for row in rows
        ]

    def list_batch_statuses(self) -> list[str]:
        rows = self.connection.execute("SELECT status FROM batches ORDER BY id").fetchall()
        return [row["status"] for row in rows]

    def close(self) -> None:
        self.connection.close()


class PipelineRunner:
    def __init__(
        self,
        database_path: Path,
        sources: dict[str, SourceClient],
        llm: LlmClient,
        pain_keywords: Sequence[str],
        batch_size: int = 15,
    ):
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self._store = _Store(Path(database_path))
        self._sources = sources
        self._llm = llm
        self._pain_keywords = tuple(keyword.casefold() for keyword in pain_keywords)
        self._batch_size = batch_size

    async def run_once(self) -> RunSummary:
        discovered = 0
        candidates: list[Post] = []
        for source_name, source in self._sources.items():
            fetched = await source.fetch()
            source_batch = fetched if isinstance(fetched, SourceBatch) else SourceBatch(fetched)
            expected_source = getattr(source, "source_name", source_name)
            for post in source_batch.posts:
                if post.source != expected_source:
                    raise ValueError("source client returned a post for another source")
                if self._store.add_post(post):
                    discovered += 1
                    if self._is_candidate(post):
                        self._store.mark_candidate(post)
                        candidates.append(post)
            if source_batch.cursor is not None:
                commit_cursor = getattr(source, "commit_cursor", None)
                if commit_cursor is None:
                    raise ValueError("source returned a cursor but cannot commit it")
                commit_cursor(source_batch.cursor)

        completed = 0
        failed = 0
        for offset in range(0, len(candidates), self._batch_size):
            batch = candidates[offset : offset + self._batch_size]
            batch_id, attempt_id = self._store.create_batch(batch)
            try:
                themes = await self._llm.analyze(batch)
            except Exception as error:  # noqa: BLE001 - failure is durable pipeline state
                self._store.fail_batch(batch_id, attempt_id, str(error))
                failed += 1
            else:
                self._store.complete_batch(batch_id, attempt_id, themes)
                completed += 1

        return RunSummary(discovered, len(candidates), completed, failed)

    def list_themes(self) -> list[Theme]:
        return self._store.list_themes()

    def list_batch_statuses(self) -> list[str]:
        return self._store.list_batch_statuses()

    def close(self) -> None:
        self._store.close()

    def _is_candidate(self, post: Post) -> bool:
        searchable = f"{post.title} {post.text}".casefold()
        return any(keyword in searchable for keyword in self._pain_keywords)
