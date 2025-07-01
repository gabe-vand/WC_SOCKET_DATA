#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Compiling…"
javac Utils.java Socket.java runSocket.java

echo "Running runSocket…"
java runSocket "$@"
