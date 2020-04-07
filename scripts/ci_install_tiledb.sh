#!/bin/bash

set -v -x

original_dir=$PWD

# Install tiledb using 2.0.0 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/TileDB-Inc/TileDB.git -b 2.0.0-rc1 && cd TileDB \
&& export deps_args="" \
&& export bootstrap_args="--enable=verbose,static-tiledb,serialization" \
&& mkdir -p build && cd build

# Configure and build TileDB
../bootstrap $bootstrap_args \
&& make -j4 \
&& make -j4 -C tiledb install

cd $original_dir

set +v +x
