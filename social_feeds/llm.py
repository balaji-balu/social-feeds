from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol, Sequence

from .hn import HttpResponse
from .pipeline import Post, Theme


@dataclass(frozen=True)
class LlmConfig:
    provider: str
    base_url: str
    api_key: str
    model: str
    timeout_seconds: float = 60
    max_input_chars: int = 12000
    supports_structured_output: bool = True
    max_attempts: int = 3
    retry_delay: float = 0.25


class LlmFailure(RuntimeError):
    def __init__(self, category: str, message: str):
        super().__init__(f"{category}: {message}")
        self.category = category


class LlmTransport(Protocol):
    async def post(self, url: str, headers: dict[str, str], payload: dict) -> HttpResponse: ...


class UrlLibLlmTransport:
    async def post(self, url: str, headers: dict[str, str], payload: dict) -> HttpResponse:
        return await asyncio.to_thread(self._post, url, headers, payload)

    @staticmethod
    def _post(url: str, headers: dict[str, str], payload: dict) -> HttpResponse:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={**headers, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
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


class OpenAiCompatibleLlm:
    def __init__(self, config: LlmConfig, transport: LlmTransport):
        if config.timeout_seconds <= 0 or config.max_input_chars < 1 or config.max_attempts < 1:
            raise ValueError("invalid LLM configuration")
        self.config = config
        self.transport = transport

    async def analyze(self, posts: Sequence[Post]) -> Sequence[Theme]:
        payload = self._payload(posts)
        response = await self._request(payload)
        try:
            return self._parse(response)
        except LlmFailure as error:
            if error.category not in {"malformed_json", "schema_validation"}:
                raise
            repair_payload = self._payload(posts, repair=True, invalid_response=response.body)
            repaired = await self._request(repair_payload)
            return self._parse(repaired)

    async def _request(self, payload: dict) -> HttpResponse:
        headers = {"Accept": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        url = f"{self.config.base_url.rstrip('/')}/chat/completions"
        for attempt in range(self.config.max_attempts):
            try:
                response = await asyncio.wait_for(
                    self.transport.post(url, headers, payload), timeout=self.config.timeout_seconds
                )
            except asyncio.TimeoutError as error:
                if attempt + 1 == self.config.max_attempts:
                    raise LlmFailure("timeout", "request timed out") from error
                await asyncio.sleep(self.config.retry_delay * (2**attempt))
                continue
            except Exception as error:  # noqa: BLE001 - bounded transport retry
                if attempt + 1 == self.config.max_attempts:
                    raise LlmFailure("transport", "request failed") from error
                await asyncio.sleep(self.config.retry_delay * (2**attempt))
                continue
            if response.status == 200:
                return response
            if response.status == 429 or response.status >= 500:
                if attempt + 1 == self.config.max_attempts:
                    category = "rate_limit" if response.status == 429 else "transport"
                    raise LlmFailure(category, f"HTTP {response.status}")
                retry_after = float(response.headers.get("Retry-After", self.config.retry_delay))
                await asyncio.sleep(max(retry_after, self.config.retry_delay * (2**attempt)))
                continue
            if response.status in {402, 403}:
                raise LlmFailure("quota", f"HTTP {response.status}")
            raise LlmFailure("transport", f"HTTP {response.status}")
        raise AssertionError("unreachable")

    def _payload(self, posts: Sequence[Post], repair: bool = False, invalid_response: str = "") -> dict:
        remaining = self.config.max_input_chars
        items = []
        for post in posts:
            excerpt = post.text[: min(len(post.text), remaining)]
            remaining -= len(excerpt)
            items.append(
                {
                    "source_id": post.source_id,
                    "source": post.source,
                    "url": post.url,
                    "title": post.title,
                    "text": excerpt,
                }
            )
            if remaining <= 0:
                break
        instruction = (
            "Repair the previous response into valid JSON matching the required schema. Return JSON only."
            if repair
            else "Group these candidate posts into recurring pain-point themes. Return JSON only."
        )
        if repair:
            instruction += f" Previous response: {invalid_response[: self.config.max_input_chars]}"
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": instruction},
                {"role": "user", "content": json.dumps({"posts": items})},
            ],
            "temperature": 0,
        }
        if self.config.supports_structured_output:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "theme_results",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["themes"],
                        "properties": {
                            "themes": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["theme", "summary", "example_posts"],
                                    "properties": {
                                        "theme": {"type": "string"},
                                        "summary": {"type": "string"},
                                        "example_posts": {"type": "array", "items": {"type": "string"}},
                                    },
                                },
                            }
                        },
                    },
                },
            }
        return payload

    @staticmethod
    def _parse(response: HttpResponse) -> list[Theme]:
        try:
            envelope = json.loads(response.body)
        except json.JSONDecodeError as error:
            raise LlmFailure("malformed_json", "provider envelope is not JSON") from error
        choices = envelope.get("choices")
        if not choices:
            raise LlmFailure("schema_validation", "provider returned no choices")
        choice = choices[0]
        message = choice.get("message") or {}
        if message.get("refusal"):
            raise LlmFailure("refusal", message["refusal"])
        if choice.get("finish_reason") == "length":
            raise LlmFailure("truncation", "provider output was truncated")
        content = message.get("content")
        if not isinstance(content, str):
            raise LlmFailure("schema_validation", "message content is not JSON text")
        try:
            result = json.loads(content)
        except json.JSONDecodeError as error:
            raise LlmFailure("malformed_json", "message content is not JSON") from error
        themes = result.get("themes") if isinstance(result, dict) else None
        if not isinstance(themes, list):
            raise LlmFailure("schema_validation", "themes must be an array")
        parsed = []
        for item in themes:
            if not isinstance(item, dict):
                raise LlmFailure("schema_validation", "theme must be an object")
            if not all(isinstance(item.get(key), str) for key in ("theme", "summary")):
                raise LlmFailure("schema_validation", "theme and summary must be strings")
            examples = item.get("example_posts")
            if not isinstance(examples, list) or not all(isinstance(value, str) for value in examples):
                raise LlmFailure("schema_validation", "example_posts must be strings")
            parsed.append(Theme(item["theme"], item["summary"], examples))
        return parsed
