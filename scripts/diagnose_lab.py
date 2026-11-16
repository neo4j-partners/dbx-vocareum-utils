#!/usr/bin/env -S uv run
"""Compatibility wrapper for the packaged lab diagnostic CLI."""

from dbx_vocareum_tools.diagnose import main

if __name__ == "__main__":
    raise SystemExit(main())
