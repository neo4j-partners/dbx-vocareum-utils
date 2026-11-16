#!/usr/bin/env -S uv run
"""Compatibility wrapper for the packaged instance-pool CLI."""

from dbx_vocareum_tools.pool import main

if __name__ == "__main__":
    raise SystemExit(main())
