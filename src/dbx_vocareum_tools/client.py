"""Safe HTTP and transaction helpers for the Vocareum API."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from .security import normalize_key, redact_text, safe_url

DEFAULT_HTTP_TIMEOUT_SECONDS = 30
DEFAULT_MAX_BODY_CHARS = 2000
USER_AGENT = "dbx-vocareum-tools/1.0"

TRANSACTION_POLL_SECONDS = 5
TRANSACTION_TIMEOUT_SECONDS = 120

# Both halves of Vocareum's transaction vocabulary. The poll below runs while
# the state is PENDING_STATE, and every caller then has to decide whether the
# state it stopped on counts as done, so the two are kept together rather than
# spelled out again in each tool.
PENDING_STATE = "pending"
TERMINAL_SUCCESS_STATES = frozenset({"success", "completed", "complete"})

T = TypeVar("T")


class ApiError(RuntimeError):
    """Raised when a Vocareum API call cannot complete."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class TransactionResult:
    """Terminal or timed-out state returned by a transaction poll."""

    state: str
    payload: Any
    timed_out: bool = False


def find_transaction_id(payload: Any) -> str | None:
    """Find a transaction ID in an undocumented response shape."""
    if isinstance(payload, dict):
        for key, value in payload.items():
            if normalize_key(key) == "transactionid" and isinstance(value, (str, int)):
                transaction_id = str(value).strip()
                if transaction_id:
                    return transaction_id
        for value in payload.values():
            found = find_transaction_id(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_transaction_id(item)
            if found is not None:
                return found
    return None


def clip_body(body: str, limit: int = DEFAULT_MAX_BODY_CHARS) -> str:
    """Return a response body clipped to a safe diagnostic length."""
    if not body:
        return "(empty)"
    if len(body) <= limit:
        return body
    return f"{body[:limit]}\n... truncated, {len(body)} characters total."


def request_json(
    method: str,
    url: str,
    api_key: str,
    body: Any = None,
    *,
    timeout: int = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> Any:
    """Call the Vocareum API and decode a JSON response safely."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Authorization", f"Token {api_key}")
    request.add_header("Accept", "application/json")
    request.add_header("User-Agent", USER_AGENT)
    if data is not None:
        request.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        detail = clip_body(redact_text(raw))
        raise ApiError(
            f"HTTP {error.code} from {method} {safe_url(url)}\nResponse body: {detail}",
            status_code=error.code,
        ) from error
    except urllib.error.URLError as error:
        raise ApiError(f"Could not reach {safe_url(url)}: {error.reason}") from error
    except TimeoutError as error:
        raise ApiError(f"Timed out reaching {safe_url(url)}") from error

    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        detail = clip_body(redact_text(raw))
        raise ApiError(
            f"Could not decode JSON from {method} {safe_url(url)}\n"
            f"Response body: {detail}"
        ) from error


def download_bytes(url: str, *, timeout: int = DEFAULT_HTTP_TIMEOUT_SECONDS) -> bytes:
    """Fetch a signed download URL without exposing its authorization values."""
    request = urllib.request.Request(url, method="GET")
    request.add_header("User-Agent", USER_AGENT)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise ApiError(
            f"HTTP {error.code} downloading {safe_url(url)}",
            status_code=error.code,
        ) from error
    except urllib.error.URLError as error:
        raise ApiError(
            f"Could not reach download URL {safe_url(url)}: {error.reason}"
        ) from error
    except TimeoutError as error:
        raise ApiError(f"Timed out reaching download URL {safe_url(url)}") from error


def poll_transaction(
    url: str,
    api_key: str,
    *,
    poll_seconds: int,
    timeout_seconds: int,
    on_state: Callable[[str], None] | None = None,
) -> TransactionResult:
    """Poll a transaction until it leaves pending or reaches the timeout."""
    deadline = time.monotonic() + timeout_seconds

    while True:
        payload = request_json("GET", url, api_key)
        state = (
            str(payload.get("state", "unknown"))
            if isinstance(payload, dict)
            else "unknown"
        )
        if on_state is not None:
            on_state(state)
        if state.lower() != PENDING_STATE:
            return TransactionResult(state=state, payload=payload)
        if time.monotonic() + poll_seconds >= deadline:
            return TransactionResult(state=state, payload=payload, timed_out=True)
        time.sleep(poll_seconds)


def wait_for_payload(
    fetch: Callable[[], Any],
    extract: Callable[[Any], T | None],
    *,
    poll_seconds: int,
    timeout_seconds: int,
    wait: bool,
    on_retry: Callable[[int], None] | None = None,
) -> tuple[Any, T | None]:
    """Re-fetch a payload until ``extract`` finds something, or time runs out.

    Returns the payload from the final attempt together with whatever was
    extracted from it, or ``None`` when nothing was. The payload comes back
    either way, because a caller that found nothing still wants to print what
    it did get. With ``wait`` false the fetch happens exactly once.
    """
    deadline = time.monotonic() + timeout_seconds

    while True:
        payload = fetch()
        found = extract(payload)
        if found is not None:
            return payload, found
        if not wait or time.monotonic() + poll_seconds >= deadline:
            return payload, None
        if on_retry is not None:
            on_retry(poll_seconds)
        time.sleep(poll_seconds)
