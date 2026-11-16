"""Offline checks of ``dbx-vocareum-probe`` that the self-test tables do not cover."""

from __future__ import annotations

from pathlib import Path

from dbx_vocareum_tools import probe

TEMPLATE = (
    Path(__file__).resolve().parent.parent / "probes" / "principal-not-found.json"
)


def test_an_unfilled_probes_template_is_refused_before_any_call(capsys):
    # The committed template is the case that matters. Its paths name
    # ${VOC_GROUP_ID} and friends, and sent as written each would answer 404,
    # which reads as NOT_FOUND_OR_HIDDEN: the very finding the file exists to
    # test. The refusal comes from collect_probes, before any config is read or
    # any credential exchanged, so this test needs no network and no .env.
    status = probe.main(["--probes-file", str(TEMPLATE)])

    error = capsys.readouterr().err
    assert status == probe.EXIT_CONFIG
    assert "${DATABRICKS_WORKSPACE_ID}" in error
    assert str(TEMPLATE) in error
