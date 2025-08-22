#!/usr/bin/env bash
set -e
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"

graph_opt=("$@")
PYTHONPATH="${BASE_DIR}:$PYTHONPATH" python3 scripts/create_session_dependency_graph.py "${graph_opt[@]}"
