set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.19.1/tiledb-linux-x86_64-2.19.1-29ceb3e7.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
sudo ldconfig /usr/local/lib
