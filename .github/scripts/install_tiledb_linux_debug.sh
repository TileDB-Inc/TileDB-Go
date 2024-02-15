set -e -x
git clone https://github.com/TileDB-Inc/TileDB.git -b 2.20.1
cd TileDB
mkdir build && cd build
cmake -DTILEDB_WERROR=OFF -DTILEDB_VCPKG=ON -DSANITIZER=leak -DTILEDB_VERBOSE=OFF -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=/usr/local ..
make -j4
sudo make -C tiledb install
sudo ldconfig /usr/local/lib
