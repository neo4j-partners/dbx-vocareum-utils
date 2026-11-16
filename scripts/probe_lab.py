#!/usr/bin/env -S uv run
"""Compatibility wrapper for the packaged permission-probe CLI."""

from dbx_vocareum_tools.probe import main

if __name__ == "__main__":
    raise SystemExit(main())
