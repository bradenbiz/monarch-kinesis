#!/usr/bin/env python3
"""Convenience entrypoint: `python sync.py <command>`."""

from kinesis_monarch.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
