"""
The Hub's rate limit is a wait, not a failure.

Version 1.0 surfaced HTTP 429 as an exception, so a prefetch that outran the
five-minute request window died partway through with a half-populated cache.
These tests pin the retry behaviour that replaced it.
"""

from unittest.mock import MagicMock

import pytest

from sporc import source as source_module
from sporc.source import HubDataSource


def _http_error(status, headers=None):
    """
    An HfHubHTTPError shaped like the real one.

    huggingface_hub 1.x carries an httpx.Response and requires it at
    construction, so this builds a genuine response rather than a stand-in --
    the retry reads status_code and Retry-After straight off it.
    """
    import httpx
    from huggingface_hub.errors import HfHubHTTPError

    return HfHubHTTPError(
        "rate limited",
        response=httpx.Response(
            status,
            headers=headers or {},
            # httpx refuses to expose parts of a response with no request
            # attached, and HfHubHTTPError reads them while building its message.
            request=httpx.Request("GET", "https://huggingface.co"),
        ),
    )


@pytest.fixture
def no_sleep(monkeypatch):
    """Run the backoff without actually waiting."""
    slept = []
    monkeypatch.setattr(source_module.time, "sleep", slept.append)
    return slept


class TestRateLimitRetry:
    def test_retries_and_succeeds(self, tmp_path, no_sleep):
        src = HubDataSource("blitt/SPoRC", str(tmp_path))
        calls = []

        def flaky(**kw):
            calls.append(kw["filename"])
            if len(calls) < 3:
                raise _http_error(429)
            return "/fake/local/path"

        assert src._download_with_retry(flaky, "turns/text/p.parquet") == \
            "/fake/local/path"
        assert len(calls) == 3
        # Backoff grows rather than hammering the same closed window.
        assert no_sleep == [10.0, 20.0]

    def test_honours_retry_after_header(self, tmp_path, no_sleep):
        src = HubDataSource("blitt/SPoRC", str(tmp_path))
        state = {"n": 0}

        def flaky(**kw):
            state["n"] += 1
            if state["n"] == 1:
                raise _http_error(429, {"Retry-After": "42"})
            return "/fake/local/path"

        src._download_with_retry(flaky, "episodes/p.parquet")
        # The server knows when its window reopens; prefer its number.
        assert no_sleep == [42.0]

    def test_caps_the_wait(self, tmp_path, no_sleep):
        src = HubDataSource("blitt/SPoRC", str(tmp_path))
        state = {"n": 0}

        def flaky(**kw):
            state["n"] += 1
            if state["n"] == 1:
                raise _http_error(429, {"Retry-After": "99999"})
            return "/fake/local/path"

        src._download_with_retry(flaky, "episodes/p.parquet")
        assert no_sleep == [source_module._RETRY_MAX_SECONDS]

    def test_gives_up_eventually(self, tmp_path, no_sleep):
        """A permanent problem must not retry forever."""
        from huggingface_hub.errors import HfHubHTTPError

        src = HubDataSource("blitt/SPoRC", str(tmp_path))
        calls = []

        def always_limited(**kw):
            calls.append(1)
            raise _http_error(429)

        with pytest.raises(HfHubHTTPError):
            src._download_with_retry(always_limited, "episodes/p.parquet")
        assert len(calls) == source_module._MAX_RETRIES

    def test_other_http_errors_are_not_retried(self, tmp_path, no_sleep):
        """401 or 404 will not fix itself; failing fast is the useful answer."""
        from huggingface_hub.errors import HfHubHTTPError

        src = HubDataSource("blitt/SPoRC", str(tmp_path))
        calls = []

        def unauthorized(**kw):
            calls.append(1)
            raise _http_error(401)

        with pytest.raises(HfHubHTTPError):
            src._download_with_retry(unauthorized, "episodes/p.parquet")
        assert len(calls) == 1
        assert no_sleep == []
