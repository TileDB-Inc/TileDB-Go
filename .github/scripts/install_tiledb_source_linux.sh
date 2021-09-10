set -e -x
git clone -b bd/ch9808-schema-evolution-serialization https://github.com/TileDB-Inc/TileDB.git
cd TileDB
mkdir build && cd build
cmake -DTILEDB_VERBOSE=OFF -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ..
make -j$(nproc)
sudo make -C tiledb install
sudo ldconfig
