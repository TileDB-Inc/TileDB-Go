set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.6.0/tiledb-macos-x86_64-2.6.0-66f4b41.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
