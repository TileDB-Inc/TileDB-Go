#!/bin/bash

set -v -x

original_dir=$PWD

# Install tiledb using 1.6 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/TileDB-Inc/TileDB.git -b 1.6.0 && cd TileDB \
&& export deps_args="" \
&& export bootstrap_args="--enable=verbose,static-tiledb,serialization" \
&& mkdir -p build && cd build

# Configure and build TileDB
../bootstrap $bootstrap_args \
&& make -j4 \
&& make -C tiledb install

cd $original_dir

set +v +x
