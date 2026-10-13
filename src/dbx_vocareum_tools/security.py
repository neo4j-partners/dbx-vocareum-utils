"""Redaction helpers for values returned by Vocareum, Databricks, and signed URLs.

Three families of credential pass through these tools. Vocareum returns signed
download URLs carrying a ``signature``, and its session record for a cloud lab
carries a console sign-in token and an unencrypted SSH private key. Databricks
issues personal access tokens prefixed ``dapi``, service principal OAuth
secrets prefixed ``dose``, and OAuth access tokens that are JWTs. The AWS
helpers below are retained because a Vocareum Databricks part still runs in an
AWS account, and the shape of the session record it returns is undocumented, so
an ``aws_data`` block appearing in one is possible rather than ruled out.

Redaction is by name fragment and by value shape together. Name matching alone
misses a token that arrives inside a prose error message, and shape matching
alone misses a field whose name says secret and whose value is short enough to
look like anything.

Name matching is deliberately broad, so it catches field names nobody has seen
yet. The cost is that it also catches a field whose name reads like a secret and
whose value is not one, and a masked value is unreadable to the run that needed
it. ``NOT_SECRET_NAMES`` is the exemption list for those, and it exempts the
name only: an exempted field's value still goes through shape matching, so a
credential that turns up in one is still caught.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from typing import Any

REDACTED = "********"

SECRET_NAME_FRAGMENTS = (
    "secret",
    "password",
    "passwd",
    "passphrase",
    "privatekey",
    "sshkey",
    "token",
    "credential",
    "signature",
    "apikey",
    # Databricks. "clientsecret" already matches "secret", but "oauth" and
    # "authorization" do not, and both name fields the SDK and the account API
    # populate with live values.
    "oauth",
    "authorization",
    "bearer",
)

# Field names that match a fragment above and hold no secret. An entry wins over
# the fragments, and it exempts the name only: the value still goes through shape
# matching, so an exempted field carrying a real credential is still caught.
#
# Written normalized, the way normalize_key leaves a key, because that is the
# form is_secret_name compares. So "credentials_found" is entered as
# "credentialsfound", and the match is exact rather than a fragment: an
# exemption that spread by substring would be a way to smuggle a secret past
# this module by naming it well.
#
# credentialsfound: written by voc_detect_credential in labruntime/voclib.sh into
# voccustomdata.txt, and read back out of the session record by
# dbx-vocareum-diagnose. It names which credentials and tools a lifecycle script
# could reach, never the value of one, and it is the answer the first live lab
# start exists to get. Without this entry it matches "credential" and comes back
# masked from both --raw-record and --json, which is the one field in the record
# that cannot be recovered any other way: Vocareum does not expose a lifecycle
# script's standard output through its REST API.
NOT_SECRET_NAMES = frozenset({"credentialsfound"})

SENSITIVE_QUERY_PARAMS = frozenset(
    {
        "accesstoken",
        "apikey",
        "credential",
        "password",
        "sessiontoken",
        "securitytoken",
        "signature",
        "signintoken",
        "token",
        "xamzcredential",
        "xamzsecuritytoken",
        "xamzsignature",
    }
)

# The second alternative covers a body that was clipped before the END line.
# It is deliberately narrow, whole lines of base64 and nothing else, because a
# greedy fallback would be just as safe but would also swallow whatever follows
# the key. What follows is usually the failure reason, which is the one thing
# these bodies are read for.
#
# A base64 line and a single prose word are the same thing to a regex, so a
# one-word line immediately under the key is redacted with it. That costs a word
# rather than the rest of the message, which is the trade being made here.
PEM_BLOCK_PATTERN = re.compile(
    r"-----BEGIN[^-\n]*PRIVATE KEY-----"
    r"(?:.*?-----END[^-\n]*PRIVATE KEY-----|(?:\r?\n[A-Za-z0-9+/=]+)*)",
    re.DOTALL,
)
JSON_SECRET_FIELD_PATTERN = re.compile(
    r'("[A-Za-z0-9_.\-]*'
    r"(?:secret|token|password|passphrase|private_?key|ssh_?key|credential"
    r'|signature|api_?key)[A-Za-z0-9_.\-]*"\s*:\s*)"(?:[^"\\]|\\.)*"',
    re.IGNORECASE,
)
SENSITIVE_QUERY_PATTERN = re.compile(
    r"((?:signin|session|security|access)?token"
    r"|x-amz-(?:credential|security-token|signature)"
    r"|credential|signature|api-?key)=([^&\s\"'\\]+)",
    re.IGNORECASE,
)

# Databricks credentials matched by the shape of the value rather than by the
# name of the field holding it, because a field name is often not available.
# The API quotes the token it rejected in the error body, and an SDK traceback
# prints the Authorization header it sent. Both arrive as prose.
#
# Each prefix is Databricks' own and is not shared with anything else the tools
# print, so the alternatives can be anchored on it rather than on a generic
# run of base64, which would also swallow ordinary identifiers.
DATABRICKS_CREDENTIAL_PATTERN = re.compile(
    r"\b(?:"
    r"dapi[0-9a-f]{32}(?:-\d{1,2})?"  # personal access token
    r"|dose[0-9a-zA-Z]{16,}"  # service principal OAuth secret
    # An OAuth access token is a JWT. Three dot-separated base64url segments,
    # with the leading "eyJ" that every base64-encoded JSON header begins with.
    r"|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
    r")"
)


def normalize_key(key: str) -> str:
    """Lowercase a key and strip every non-alphanumeric character."""
    return "".join(char for char in key.lower() if char.isalnum())


def is_access_key_id(key: str) -> bool:
    """Report whether a key name resembles an AWS access key ID."""
    normalized = normalize_key(key)
    return "accesskeyid" in normalized or normalized in {
        "accesskey",
        "awsaccesskey",
        "keyid",
    }


def mask_secret(value: Any, keep: int = 4) -> str:
    """Return a secret reduced to its length and its last few characters.

    The length is shown rather than one asterisk per character, because a
    session token runs to a couple of thousand characters and a faithful mask
    would bury the rest of the output. The tail is shown rather than the head so
    two secrets can still be told apart in a transcript without printing the
    prefix, which is the half that identifies what kind of credential it is.
    """
    if not isinstance(value, str) or len(value) <= keep:
        return REDACTED
    return f"[{len(value)} chars]{REDACTED}{value[-keep:]}"


def is_private_key(key: str) -> bool:
    """Report whether a key name resembles private key material."""
    normalized = normalize_key(key)
    return "privatekey" in normalized or "sshkey" in normalized


def is_secret_name(key: str) -> bool:
    """Report whether a field name marks a secret value."""
    if is_access_key_id(key):
        return False
    normalized = normalize_key(key)
    if normalized in NOT_SECRET_NAMES:
        return False
    return any(fragment in normalized for fragment in SECRET_NAME_FRAGMENTS)


def redact_url(value: str) -> str:
    """Replace sensitive query values in a URL."""
    if "://" not in value or "?" not in value:
        return value
    parts = urllib.parse.urlsplit(value)
    pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    if not any(normalize_key(name) in SENSITIVE_QUERY_PARAMS for name, _ in pairs):
        return value
    redacted = [
        (name, REDACTED if normalize_key(name) in SENSITIVE_QUERY_PARAMS else item)
        for name, item in pairs
    ]
    return urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urllib.parse.urlencode(redacted),
            parts.fragment,
        )
    )


def redact_credential_values(text: str) -> str:
    """Replace Databricks credentials found by value shape rather than by name."""
    return DATABRICKS_CREDENTIAL_PATTERN.sub(
        lambda match: mask_secret(match.group(0)), text
    )


def redact_payload(payload: Any, key: str = "") -> Any:
    """Return a copy of a payload with secret values replaced."""
    if isinstance(payload, dict):
        return {name: redact_payload(value, name) for name, value in payload.items()}
    if isinstance(payload, list):
        return [redact_payload(item, key) for item in payload]
    if isinstance(payload, str):
        if is_private_key(key) or (
            "-----BEGIN" in payload and "PRIVATE KEY" in payload
        ):
            return f"{REDACTED} (private key withheld, len={len(payload)})"
        if is_secret_name(key):
            return mask_secret(payload)
        return redact_credential_values(redact_url(payload))
    if is_private_key(key) or is_secret_name(key):
        return REDACTED
    return payload


def redact_text(body: str) -> str:
    """Redact secrets from an unstructured response body."""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        pass
    else:
        return json.dumps(redact_payload(payload), sort_keys=True)

    body = JSON_SECRET_FIELD_PATTERN.sub(rf'\1"{REDACTED}"', body)
    body = PEM_BLOCK_PATTERN.sub(f"{REDACTED} (private key withheld)", body)
    body = SENSITIVE_QUERY_PATTERN.sub(rf"\1={REDACTED}", body)
    return redact_credential_values(body)


def safe_url(url: str) -> str:
    """Return a URL safe to include in diagnostics."""
    return redact_url(url)
