#!/bin/bash

set -v -x

original_dir=$PWD

# Install tiledb using 1.7.7 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/TileDB-Inc/TileDB.git -b 1.7.7 && cd TileDB \
&& export deps_args="" \
&& export bootstrap_args="--enable=verbose,static-tiledb,serialization" \
&& mkdir -p build && cd build

# Configure and build TileDB
../bootstrap $bootstrap_args \
&& make -j4 \
&& make -j4 -C tiledb install

cd $original_dir

set +v +x
