#!/bin/bash
# Convenience launcher for the flight monitor.
cd "$(dirname "$0")" || exit 1
exec python3 -W ignore monitor.py "$@"
