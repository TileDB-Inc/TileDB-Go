set -e -x
git clone https://github.com/TileDB-Inc/TileDB.git -b 2.20.0-rc2
cd TileDB
mkdir build && cd build
cmake -DTILEDB_WERROR=OFF -DTILEDB_VCPKG=ON -DTILEDB_VERBOSE=OFF -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ..
make -j$(nproc)
sudo make -C tiledb install
sudo ldconfig
