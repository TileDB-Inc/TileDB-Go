set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.11.0-rc1/tiledb-macos-x86_64-2.11.0-rc1-34e5dbc.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
