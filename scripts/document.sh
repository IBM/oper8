#!/usr/bin/env bash

set -e
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"

mkdocs_opt=("$@")

if [[ ${#mkdocs_opt[@]} -eq 0 ]]; then
    echo "No options provided. Running 'mkdocs build'..."
    mkdocs build
    exit 0
fi

# Modify set e because serve will be aborted with ctrl C.
if [[ "${mkdocs_opt[0]}" == "serve" ]]; then
    set +e
    echo "Serving the documentation. Abort with ctrl C."
fi

echo "Running 'mkdocs ${mkdocs_opt[@]}'..."
mkdocs "${mkdocs_opt[@]}"
