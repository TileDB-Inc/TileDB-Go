set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.3.0-rc0/tiledb-macos-2.3.0-rc0-32eb192-full.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
