set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.7.0-rc0/tiledb-macos-x86_64-2.7.0-rc0-c7053ca.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
