#!/bin/bash
set -exo pipefail
. "${BASH_SOURCE%/*}/env.sh"
mkdir "$CORE_ROOT"
cd "$CORE_ROOT"
case "$OS" in
  linux)
    NPROC="$(nproc)"
  ;;
  macos)
    NPROC="$(sysctl -n hw.ncpu)"
  ;;
esac
git clone --depth 1 -b "$CORE_VERSION" https://github.com/TileDB-Inc/TileDB.git
cd TileDB
mkdir build
cd build
# BUILD_FLAGS is unquoted because it needs to expand into multiple strings.
cmake $BUILD_FLAGS -DCMAKE_INSTALL_PREFIX="${CORE_ROOT}/install" ..
make -j"$NPROC"

mkdir "${CORE_ROOT}/install"
make -C tiledb install
