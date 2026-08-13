from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Mapping

from .pipeline import PipelineRunner, SourceBatch, SourceClient


@dataclass(frozen=True)
class RuntimeConfig:
    queue_capacity: int = 100
    poll_interval_seconds: float = 60
    flush_interval_seconds: float = 900
    shutdown_timeout_seconds: float = 30
    batch_size: int = 15
    log_level: str = "INFO"

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "RuntimeConfig":
        values = environment or os.environ
        config = cls(
            queue_capacity=int(values.get("SOCIAL_FEEDS_QUEUE_CAPACITY", "100")),
            poll_interval_seconds=float(values.get("SOCIAL_FEEDS_POLL_INTERVAL", "60")),
            flush_interval_seconds=float(values.get("SOCIAL_FEEDS_FLUSH_INTERVAL", "900")),
            shutdown_timeout_seconds=float(values.get("SOCIAL_FEEDS_SHUTDOWN_TIMEOUT", "30")),
            batch_size=int(values.get("SOCIAL_FEEDS_BATCH_SIZE", "15")),
            log_level=values.get("SOCIAL_FEEDS_LOG_LEVEL", "INFO"),
        )
        if (
            config.queue_capacity < 1
            or config.poll_interval_seconds <= 0
            or config.flush_interval_seconds <= 0
            or config.shutdown_timeout_seconds <= 0
            or config.batch_size < 1
        ):
            raise ValueError("runtime configuration values must be positive")
        return config


@dataclass(frozen=True)
class Credentials:
    llm_api_key: str
    reddit_client_id: str
    reddit_client_secret: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Credentials":
        values = environment or os.environ
        return cls(
            llm_api_key=values.get("LLM_API_KEY", ""),
            reddit_client_id=values.get("REDDIT_CLIENT_ID", ""),
            reddit_client_secret=values.get("REDDIT_CLIENT_SECRET", ""),
        )


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        result = {"level": record.levelname, "event": record.getMessage(), "logger": record.name}
        details = getattr(record, "details", None)
        if details is not None:
            result["details"] = details
        return json.dumps(result)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


class PipelineService:
    def __init__(self, runner: PipelineRunner, config: RuntimeConfig, logger: logging.Logger | None = None):
        self.runner = runner
        self.config = config
        self.logger = logger or logging.getLogger("social_feeds.service")
        self._stop = asyncio.Event()
        self._queue: asyncio.Queue[tuple[str, SourceClient, Sequence | SourceBatch]] = asyncio.Queue(
            maxsize=config.queue_capacity
        )

    async def run(self) -> None:
        self.logger.info("pipeline_startup")
        pollers = [
            asyncio.create_task(self._poll_source(source_name, source))
            for source_name, source in self.runner.sources.items()
        ]
        consumer = asyncio.create_task(self._consume())
        try:
            await self._stop.wait()
        finally:
            for task in pollers:
                task.cancel()
            await asyncio.gather(*pollers, return_exceptions=True)
            try:
                await asyncio.wait_for(self._queue.join(), self.config.shutdown_timeout_seconds)
                await asyncio.wait_for(consumer, self.config.shutdown_timeout_seconds)
            except asyncio.TimeoutError:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
            self.logger.info("pipeline_shutdown")

    def stop(self) -> None:
        self._stop.set()

    async def _poll_source(self, source_name: str, source: SourceClient) -> None:
        while not self._stop.is_set():
            try:
                fetched = await source.fetch()
                await self._queue.put((source_name, source, fetched))
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - source isolation is intentional
                self.logger.error("source_failure:%s", error)
            try:
                await asyncio.wait_for(self._stop.wait(), self.config.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def _consume(self) -> None:
        pending = []
        discovered = 0
        while not self._stop.is_set() or not self._queue.empty():
            try:
                source_name, source, fetched = await asyncio.wait_for(
                    self._queue.get(), self.config.flush_interval_seconds
                )
            except asyncio.TimeoutError:
                if pending:
                    completed, failed = await self.runner.process_candidates(pending)
                    self._log_summary(discovered, len(pending), completed, failed)
                    pending = []
                    discovered = 0
                continue
            try:
                new_posts, candidates = self.runner.ingest_source(source_name, source, fetched)
                discovered += new_posts
                pending.extend(candidates)
                if len(pending) >= self.config.batch_size:
                    completed, failed = await self.runner.process_candidates(pending)
                    self._log_summary(discovered, len(pending), completed, failed)
                    pending = []
                    discovered = 0
            finally:
                self._queue.task_done()
        if pending:
            completed, failed = await self.runner.process_candidates(pending)
            self._log_summary(discovered, len(pending), completed, failed)

    def _log_summary(self, discovered: int, candidates: int, completed: int, failed: int) -> None:
        self.logger.info(
            "operation_summary",
            extra={
                "details": {
                    "discovered_posts": discovered,
                    "candidates": candidates,
                    "completed_batches": completed,
                    "failed_batches": failed,
                }
            },
        )
