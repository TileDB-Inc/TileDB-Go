#!/bin/bash
set -exo pipefail
. "${BASH_SOURCE%/*}/env.sh"

# Sets up the environment after installing or extracting TileDB.

if [ "$OS" = "linux" ]; then
  /sbin/ldconfig -n "${CORE_ROOT}/install/lib"
fi

go env -w "CGO_CFLAGS=-I${CORE_ROOT}/install/include"
go env -w "CGO_LDFLAGS=-L${CORE_ROOT}/install/lib/ -Wl,-rpath,${CORE_ROOT}/install/lib/"
