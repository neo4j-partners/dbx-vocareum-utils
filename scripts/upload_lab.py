#!/usr/bin/env -S uv run
"""Compatibility wrapper for the packaged lab upload CLI."""

from dbx_vocareum_tools.upload import main

if __name__ == "__main__":
    raise SystemExit(main())
