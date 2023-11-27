#!/usr/bin/env bash

cd $(dirname ${BASH_SOURCE[0]})/..

fnames=""
for fname in $@
do
    if [[ "$fname" == *".py" ]] || [ -d $fname ] && [[ "$fname" == "oper8"* ]]
    then
        fnames="$fnames $fname"
    else
        echo "Ignoring non-library file: $fname"
    fi
done
if [ "$fnames" == "" ]
then
    fnames="oper8"
fi

ruff check $arg $fnames
