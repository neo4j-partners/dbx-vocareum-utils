#!/usr/bin/env -S uv run
"""Compatibility wrapper for the packaged orphaned-home-folder CLI."""

from dbx_vocareum_tools.orphans import main

if __name__ == "__main__":
    raise SystemExit(main())
