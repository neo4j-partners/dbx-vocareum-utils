"""Redaction tests.

A Vocareum session record and a Databricks error body both carry live
credentials, and both get printed by these tools. Every case here is a value
that reached a terminal transcript in some form during development, or that the
API documents itself as returning.
"""

from __future__ import annotations

import json

import pytest

from dbx_vocareum_tools.security import (
    REDACTED,
    is_secret_name,
    mask_secret,
    redact_credential_values,
    redact_payload,
    redact_text,
    safe_url,
)

# The three Databricks credential shapes, each matched by the shape of the value
# rather than by the name of the field holding it, because a field name is often
# not available: the API quotes the token it rejected in the error body, and an
# SDK traceback prints the Authorization header it sent.
PAT = "dapi" + "0123456789abcdef" * 2
OAUTH_SECRET = "dose" + "AbCdEf0123456789xyz"
JWT = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI3NDM1YzNiZiJ9.c2lnbmF0dXJlX2hlcmVfb2s"


@pytest.mark.parametrize("credential", [PAT, OAUTH_SECRET, JWT])
def test_databricks_credentials_are_redacted_by_shape(credential: str) -> None:
    body = f"Invalid access token: {credential}"
    redacted = redact_credential_values(body)
    assert credential not in redacted
    assert REDACTED in redacted


def test_an_ordinary_identifier_survives_redaction() -> None:
    """Over-redaction costs as much as under-redaction, in a different currency.

    A workspace ID or a client ID that comes back masked makes a diagnostic
    unusable for the thing it exists to do, so the patterns anchor on Databricks'
    own prefixes rather than on a generic run of base64.
    """
    body = "workspace 3333333333333333, client 11111111-2222-4333-8444-555555555555"
    assert redact_credential_values(body) == body


def test_a_secret_field_is_masked_by_name() -> None:
    payload = {"client_id": "11111111", "client_secret": OAUTH_SECRET}
    redacted = redact_payload(payload)
    assert redacted["client_id"] == "11111111"
    assert OAUTH_SECRET not in json.dumps(redacted)


def test_a_private_key_is_withheld_rather_than_masked() -> None:
    """A masked private key still shows its length and its first characters.

    Vocareum's AWS session record carries an unencrypted SSH private key, and the
    Databricks record's shape is undocumented, so it is treated the same way.
    """
    pem = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEowIBAAKCAQEAxyz\n"
        "-----END RSA PRIVATE KEY-----\n"
    )
    redacted = redact_payload({"sshkey": pem})
    assert "MIIEowIBAAKCAQEAxyz" not in redacted["sshkey"]
    assert "private key withheld" in redacted["sshkey"]


def test_a_nested_credential_is_reached() -> None:
    """The record nests, and the nesting is not stable enough to rely on."""
    payload = {"labs": [{"databricks": {"token": PAT}}]}
    assert PAT not in json.dumps(redact_payload(payload))


def test_a_signin_token_in_a_url_is_removed() -> None:
    url = "https://signin.aws.amazon.com/federation?Action=login&SigninToken=abc123xyz"
    assert "abc123xyz" not in safe_url(url)


def test_redact_text_handles_a_body_that_is_not_json() -> None:
    """A proxy error or an HTML error page arrives as prose, not as JSON."""
    body = f"<html><body>rejected token {PAT}</body></html>"
    redacted = redact_text(body)
    assert PAT not in redacted


def test_redact_text_handles_a_body_that_is_json() -> None:
    body = json.dumps({"error_code": "UNAUTHENTICATED", "message": f"bad {PAT}"})
    redacted = redact_text(body)
    assert PAT not in redacted
    # The error code has to survive, because it is what the classifier reads.
    assert "UNAUTHENTICATED" in redacted


CREDENTIALS_FOUND = "~/.databrickscfg databricks-cli curl python3"


def test_credentials_found_is_not_masked_by_its_name() -> None:
    """The one field in the session record that cannot be recovered another way.

    ``credentials_found`` matches the "credential" fragment, and it holds the
    names of credentials and tools rather than the value of one. Masking it makes
    the first live lab start report nothing about the thing it was run to
    measure, and Vocareum does not expose a lifecycle script's standard output
    through its REST API, so there is no second copy to read.
    """
    assert is_secret_name("credentials_found") is False
    redacted = redact_payload({"credentials_found": CREDENTIALS_FOUND})
    assert redacted["credentials_found"] == CREDENTIALS_FOUND


def test_credentials_found_is_reached_where_the_record_nests_it() -> None:
    """The record's shape is undocumented, so the exemption cannot assume a path."""
    payload = {
        "resources": [{"databricks": [{"credentials_found": CREDENTIALS_FOUND}]}]
    }
    assert CREDENTIALS_FOUND in json.dumps(redact_payload(payload))


def test_an_exempt_name_still_has_its_value_checked_for_shape() -> None:
    """The exemption is on the name. A credential in one is still caught.

    This is what keeps the exemption from being a way to smuggle a secret past
    the module by naming the field well.
    """
    redacted = redact_payload({"credentials_found": f"DATABRICKS_TOKEN={PAT}"})
    assert PAT not in redacted["credentials_found"]


@pytest.mark.parametrize("key", ["found_credentials", "credentials_found_token"])
def test_the_exemption_does_not_spread_by_substring(key: str) -> None:
    """Exact match, not a fragment, or the exemption becomes a naming trick."""
    assert is_secret_name(key) is True
    assert redact_payload({key: OAUTH_SECRET})[key] != OAUTH_SECRET


def test_the_whole_custom_data_file_survives_as_a_json_body() -> None:
    """How the field arrives if the record carries voccustomdata.txt whole."""
    body = json.dumps(
        {"script": "user_setup.sh", "credentials_found": CREDENTIALS_FOUND}
    )
    redacted = redact_text(body)
    assert CREDENTIALS_FOUND in redacted
    assert "user_setup.sh" in redacted


def test_mask_secret_keeps_a_recognizable_tail() -> None:
    """Enough to confirm which secret it was, not enough to use it."""
    masked = mask_secret("dose1234567890abcdef")
    assert masked.endswith("cdef")
    assert "1234567890" not in masked
    assert "20 chars" in masked
