set -e -x
# TODO: Revert change to use fork release from shaunrd0/TileDB.
# https://linear.app/tiledb/issue/CLOUD-1043/revert-patched-core-version-used-in-tiledb-server
git clone https://github.com/shaunrd0/TileDB.git -b ${CORE_VERSION}
cd TileDB
mkdir build && cd build
cmake -DTILEDB_WERROR=OFF -DTILEDB_VCPKG=ON -DSANITIZER=leak -DTILEDB_VERBOSE=OFF -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=/usr/local ..
sudo make -j4 tiledb install-tiledb
sudo ldconfig /usr/local/lib
