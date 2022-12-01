set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.13.0/tiledb-macos-x86_64-2.13.0-db00e70.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
