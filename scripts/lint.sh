#!/usr/bin/env bash

cd $(dirname ${BASH_SOURCE[0]})/..

arg=${1:-"oper8"}
shift
ruff check $arg $@
