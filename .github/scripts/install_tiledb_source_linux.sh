set -e -x
git clone https://github.com/TileDB-Inc/TileDB
cd TileDB
git checkout 18846b62b10ac654453b9f3f3ce54900e410877c
mkdir build && cd build
cmake -DTILEDB_VERBOSE=OFF -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ..
make -j$(nproc)
sudo make -C tiledb install
sudo ldconfig