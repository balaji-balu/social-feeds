from __future__ import annotations

import re
import sqlite3
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from .hn import HttpTransport
from .pipeline import Post, SourceBatch


ATOM_NS = "{http://www.w3.org/2005/Atom}"
_TAG_RE = re.compile(r"<[^>]+>")


class RedditSourceError(RuntimeError):
    pass


class RedditRssSource:
    source_name = "reddit"

    def __init__(
        self,
        subreddit: str,
        transport: HttpTransport,
        database_path: Path,
        max_title_chars: int = 500,
        max_text_chars: int = 4000,
        retention_hours: int = 48,
        user_agent: str = "social-feeds/0.1",
        now: Callable[[], datetime] | None = None,
    ):
        if not subreddit or max_title_chars < 1 or max_text_chars < 1 or retention_hours < 1:
            raise ValueError("invalid Reddit RSS source configuration")
        self.subreddit = subreddit
        self.transport = transport
        self.database_path = Path(database_path)
        self.max_title_chars = max_title_chars
        self.max_text_chars = max_text_chars
        self.retention_hours = retention_hours
        self.user_agent = user_agent
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.metadata_key = f"reddit:{subreddit}"
        self.url = f"https://www.reddit.com/r/{subreddit}/.rss"

    async def fetch(self) -> SourceBatch:
        validators = self._validators()
        headers = {"User-Agent": self.user_agent}
        if validators.get("etag"):
            headers["If-None-Match"] = validators["etag"]
        if validators.get("last_modified"):
            headers["If-Modified-Since"] = validators["last_modified"]
        response = await self.transport.get(self.url, {}, headers)
        if response.status == 304:
            return SourceBatch(())
        if response.status != 200:
            raise RedditSourceError(f"Reddit RSS request failed with HTTP {response.status}")
        try:
            root = ElementTree.fromstring(response.body)
        except ElementTree.ParseError as error:
            raise RedditSourceError("Reddit RSS returned malformed Atom") from error

        posts = []
        for entry in root.findall(f"{ATOM_NS}entry"):
            source_id = self._text(entry, "id")
            if not source_id:
                continue
            title = self._text(entry, "title")[: self.max_title_chars]
            link = next(
                (element.attrib["href"] for element in entry.findall(f"{ATOM_NS}link") if "href" in element.attrib),
                "",
            )
            updated = self._text(entry, "updated") or self._text(entry, "published")
            content = self._text(entry, "content") or self._text(entry, "summary")
            posts.append(
                Post(
                    source=self.source_name,
                    source_id=source_id,
                    title=title,
                    url=link,
                    text=_TAG_RE.sub("", content)[: self.max_text_chars],
                    published_at=updated or None,
                )
            )
        metadata = {
            "etag": self._header(response.headers, "etag"),
            "last_modified": self._header(response.headers, "last-modified"),
        }
        return SourceBatch(tuple(posts), metadata=metadata)

    def commit_metadata(self, metadata: dict[str, str]) -> None:
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute(
                """
                INSERT INTO source_metadata(source_key, etag, last_modified)
                VALUES (?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    etag = excluded.etag,
                    last_modified = excluded.last_modified
                """,
                (self.metadata_key, metadata.get("etag", ""), metadata.get("last_modified", "")),
            )
            connection.commit()
        finally:
            connection.close()

    def expire_content(self, now: datetime | None = None) -> None:
        current = now or self.now()
        cutoff = current - timedelta(hours=self.retention_hours)
        cutoff_text = cutoff.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute(
                """
                UPDATE posts SET title = '[expired]', text = ''
                WHERE source = 'reddit' AND published_at IS NOT NULL AND published_at < ?
                """,
                (cutoff_text,),
            )
            connection.commit()
        finally:
            connection.close()

    def _validators(self) -> dict[str, str]:
        connection = sqlite3.connect(self.database_path)
        try:
            row = connection.execute(
                "SELECT etag, last_modified FROM source_metadata WHERE source_key = ?",
                (self.metadata_key,),
            ).fetchone()
        finally:
            connection.close()
        return {"etag": row[0], "last_modified": row[1]} if row else {}

    @staticmethod
    def _text(element: ElementTree.Element, name: str) -> str:
        child = element.find(f"{ATOM_NS}{name}")
        return "".join(child.itertext()).strip() if child is not None else ""

    @staticmethod
    def _header(headers: dict[str, str], name: str) -> str:
        return next((value for key, value in headers.items() if key.lower() == name), "")
