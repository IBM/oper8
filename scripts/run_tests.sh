#!/usr/bin/env bash

set -e
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"

allow_warnings=${ALLOW_WARNINGS:-"0"}
if [ "$allow_warnings" = "1" ]
then
    warn_arg=""
else
    warn_arg="-W error"
fi

PYTHONPATH="${BASE_DIR}:$PYTHONPATH" python3 -m pytest \
    --cov-config=.coveragerc \
    --cov=oper8 \
    --cov-report=term \
    --cov-report=html \
    --cov-fail-under=85.00 \
    $warn_arg "$@"
