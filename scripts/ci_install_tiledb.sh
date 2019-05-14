#!/bin/bash

set -v -x

original_dir=$PWD

# Install tiledb using git dev branch until 1.3 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/Shelnutt2/TileDB.git -b serialization-0.0.1 && cd TileDB \
&& export deps_args="" \
&& export bootstrap_args="--enable=verbose,static-tiledb" \
&& mkdir -p build && cd build

# Configure and build TileDB
../bootstrap $bootstrap_args \
&& make -j4 \
&& make -C tiledb install

cd $original_dir

set +v +x
