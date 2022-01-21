set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.6.1/tiledb-macos-x86_64-2.6.1-2f6b7f6.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
