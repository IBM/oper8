#!/usr/bin/env bash

set -e
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"

pytest_opts=("$@")
allow_warnings=${ALLOW_WARNINGS:-"0"}
if [ "$allow_warnings" = "1" ]; then
    warn_arg=""
else
    # NOTE: The AnsibleWatchManager raises an ImportWarning when ansible imports
    #   systemd under the hood
    warn_arg="-W error -W ignore::ImportWarning"

    # If running with 3.12 or later, some of the dependencies use deprecated
    # functionality
    if [ "$(python --version | cut -d' ' -f 2 | cut -d'.' -f 2)" -gt "11" ]; then
        warn_arg="$warn_arg -W ignore::DeprecationWarning"
    fi
fi

# Show the test coverage when running the whole test, otherwise omit.
if [[ "${pytest_opts[*]}" != *"tests/"* ]]; then
    pytest_opts+=(
        --cov-config=.coveragerc
        --cov=oper8
        --cov-report=term
        --cov-report=html
        --cov-fail-under=85.00
    )
fi

PYTHONPATH="${BASE_DIR}:$PYTHONPATH" python3 -m pytest \
    $warn_arg "${pytest_opts[@]}"
