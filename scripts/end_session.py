#!/usr/bin/env -S uv run
"""Compatibility wrapper for the packaged session CLI."""

from dbx_vocareum_tools.session import main

if __name__ == "__main__":
    raise SystemExit(main())
