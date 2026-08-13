from __future__ import annotations

import asyncio
import json
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .pipeline import Post, SourceBatch


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: str


class HttpTransport(Protocol):
    async def get(self, url: str, params: dict[str, str], headers: dict[str, str] | None = None) -> HttpResponse: ...


class UrlLibTransport:
    async def get(self, url: str, params: dict[str, str], headers: dict[str, str] | None = None) -> HttpResponse:
        return await asyncio.to_thread(self._get, url, params, headers or {})

    @staticmethod
    def _get(url: str, params: dict[str, str], headers: dict[str, str]) -> HttpResponse:
        request = urllib.request.Request(
            f"{url}?{urllib.parse.urlencode(params)}", headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return HttpResponse(
                    response.status,
                    {key: value for key, value in response.headers.items()},
                    response.read().decode("utf-8"),
                )
        except urllib.error.HTTPError as error:
            return HttpResponse(
                error.code,
                {key: value for key, value in error.headers.items()},
                error.read().decode("utf-8"),
            )


class HnSourceError(RuntimeError):
    pass


class HnAlgoliaSource:
    source_name = "hn"

    def __init__(
        self,
        query: str,
        transport: HttpTransport,
        database_path: Path,
        hits_per_page: int = 100,
        overlap_seconds: int = 60,
        max_attempts: int = 3,
        retry_delay: float = 0.25,
        endpoint: str = "https://hn.algolia.com/api/v1/search_by_date",
    ):
        if hits_per_page < 1 or overlap_seconds < 0 or max_attempts < 1:
            raise ValueError("HN polling parameters must be positive")
        self.query = query
        self.transport = transport
        self.database_path = Path(database_path)
        self.hits_per_page = hits_per_page
        self.overlap_seconds = overlap_seconds
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.endpoint = endpoint
        self.cursor_key = f"hn:{query}"

    @property
    def cursor(self) -> int:
        connection = sqlite3.connect(self.database_path)
        try:
            row = connection.execute(
                "SELECT watermark FROM source_cursors WHERE source_key = ?", (self.cursor_key,)
            ).fetchone()
        finally:
            connection.close()
        return int(row[0]) if row else 0

    async def fetch(self) -> SourceBatch:
        watermark = self.cursor
        lower_bound = max(0, watermark - self.overlap_seconds)
        page = 0
        posts: list[Post] = []
        newest = watermark
        while True:
            response = await self._request(
                {
                    "query": self.query,
                    "tags": "story",
                    "numericFilters": f"created_at_i>={lower_bound}",
                    "page": str(page),
                    "hitsPerPage": str(self.hits_per_page),
                }
            )
            try:
                payload = json.loads(response.body)
            except json.JSONDecodeError as error:
                raise HnSourceError("HN returned malformed JSON") from error
            for hit in payload.get("hits", []):
                source_id = str(hit.get("objectID", ""))
                if not source_id:
                    continue
                created_at = int(hit.get("created_at_i") or 0)
                newest = max(newest, created_at)
                posts.append(
                    Post(
                        source=self.source_name,
                        source_id=source_id,
                        title=hit.get("title") or "",
                        url=hit.get("url") or f"https://news.ycombinator.com/item?id={source_id}",
                        text=hit.get("story_text") or hit.get("comment_text") or "",
                        published_at=hit.get("created_at"),
                    )
                )
            if page + 1 >= int(payload.get("nbPages", 0)):
                break
            page += 1
        return SourceBatch(tuple(posts), newest)

    async def _request(self, params: dict[str, str]) -> HttpResponse:
        for attempt in range(self.max_attempts):
            try:
                response = await self.transport.get(self.endpoint, params)
            except Exception as error:  # noqa: BLE001 - bounded transport retry
                if attempt + 1 == self.max_attempts:
                    raise HnSourceError("HN request failed") from error
                await asyncio.sleep(self.retry_delay * (2**attempt))
                continue
            if response.status == 200:
                return response
            if response.status == 429 or response.status >= 500:
                if attempt + 1 == self.max_attempts:
                    raise HnSourceError(f"HN request failed with HTTP {response.status}")
                retry_after = float(response.headers.get("Retry-After", self.retry_delay))
                await asyncio.sleep(max(retry_after, self.retry_delay * (2**attempt)))
                continue
            raise HnSourceError(f"HN request failed with HTTP {response.status}")
        raise AssertionError("unreachable")

    def commit_cursor(self, watermark: int) -> None:
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute(
                """
                INSERT INTO source_cursors(source_key, watermark)
                VALUES (?, ?)
                ON CONFLICT(source_key) DO UPDATE SET watermark = MAX(watermark, excluded.watermark)
                """,
                (self.cursor_key, watermark),
            )
            connection.commit()
        finally:
            connection.close()
