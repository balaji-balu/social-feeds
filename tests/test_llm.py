import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from social_feeds.hn import HttpResponse
from social_feeds.llm import LlmConfig, LlmFailure, OpenAiCompatibleLlm
from social_feeds.pipeline import PipelineRunner, Post, Theme


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def post(self, url, headers, payload):
        self.requests.append((url, headers, payload))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def valid_response():
    return HttpResponse(
        200,
        {},
        json.dumps(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "content": json.dumps(
                                {
                                    "themes": [
                                        {
                                            "theme": "Setup pain",
                                            "summary": "Users struggle with setup.",
                                            "example_posts": ["post-1"],
                                        }
                                    ]
                                }
                            )
                        },
                    }
                ]
            }
        ),
    )


class OpenAiCompatibleLlmTests(unittest.TestCase):
    def test_validates_structured_output_and_can_run_through_pipeline(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport([valid_response()])
            adapter = OpenAiCompatibleLlm(
                LlmConfig(
                    provider="test",
                    base_url="https://llm.test/v1",
                    api_key="secret",
                    model="test-model",
                    supports_structured_output=True,
                ),
                transport,
            )

            class Source:
                source_name = "fake"

                async def fetch(self):
                    return [Post("fake", "post-1", "Painful setup", "https://post", "Pain")]

            runner = PipelineRunner(
                database_path=Path(directory) / "pipeline.sqlite",
                sources={"fake": Source()},
                llm=adapter,
                pain_keywords=("pain",),
            )

            summary = asyncio.run(runner.run_once())

            self.assertEqual(summary.completed_batches, 1)
            self.assertEqual(runner.list_themes(), [Theme("Setup pain", "Users struggle with setup.", ["post-1"])])
            self.assertEqual(transport.requests[0][0], "https://llm.test/v1/chat/completions")
            self.assertIn("response_format", transport.requests[0][2])
            self.assertEqual(transport.requests[0][1]["Authorization"], "Bearer secret")
            runner.close()

    def test_retries_transient_rate_limit_and_repairs_invalid_output_once(self):
        invalid = HttpResponse(
            200,
            {},
            json.dumps({"choices": [{"finish_reason": "stop", "message": {"content": "not json"}}]}),
        )
        transport = FakeTransport([HttpResponse(429, {"Retry-After": "0"}, ""), invalid, valid_response()])
        adapter = OpenAiCompatibleLlm(
            LlmConfig(
                provider="test",
                base_url="https://llm.test/v1",
                api_key="secret",
                model="test-model",
                supports_structured_output=False,
                retry_delay=0,
            ),
            transport,
        )

        themes = asyncio.run(adapter.analyze([Post("fake", "post-1", "Pain", "https://post", "Pain")]))

        self.assertEqual(themes[0].theme, "Setup pain")
        self.assertEqual(len(transport.requests), 3)
        self.assertIn("repair", transport.requests[2][2]["messages"][0]["content"].lower())

    def test_refusal_and_truncation_are_categorized(self):
        refusal = HttpResponse(
            200,
            {},
            json.dumps({"choices": [{"finish_reason": "stop", "message": {"refusal": "No"}}]}),
        )
        adapter = OpenAiCompatibleLlm(
            LlmConfig("test", "https://llm.test/v1", "secret", "test-model"),
            FakeTransport([refusal]),
        )
        with self.assertRaisesRegex(LlmFailure, "refusal"):
            asyncio.run(adapter.analyze([Post("fake", "post-1", "Pain", "https://post")]))


if __name__ == "__main__":
    unittest.main()
