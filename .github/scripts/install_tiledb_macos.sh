set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.5.1/tiledb-macos-x86_64-2.5.1-5b65a96.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
