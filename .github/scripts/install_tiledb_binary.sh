#!/bin/bash
set -exo pipefail
. "${BASH_SOURCE%/*}/env.sh"
mkdir "$CORE_ROOT"
cd "$CORE_ROOT"
mkdir install
curl --location -o tiledb.tar.gz "https://github.com/TileDB-Inc/TileDB/releases/download/${CORE_VERSION}/tiledb-${OS}-x86_64-${CORE_VERSION}-${CORE_HASH}.tar.gz"
tar -C ./install -xf tiledb.tar.gz
