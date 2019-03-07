#!/bin/bash
if [[ -n "$(gofmt -l .)" ]]; then
    echo "TileDB-Go code is not formatted:"
    gofmt -d .
    exit 1
else
 echo "TileDB-Go code is well formatted."
fi