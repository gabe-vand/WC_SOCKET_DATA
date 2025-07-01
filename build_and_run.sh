#!/usr/bin/env bash
set -euo pipefail

# ── adjust this if your repo isn't the script's parent dir ──
cd "$(dirname "$0")"

echo "Compiling…"
javac Utils.java Socket.java runSocket.java

echo "Running runSocket…"
java runSocket "$@"
