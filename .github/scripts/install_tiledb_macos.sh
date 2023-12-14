set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.18.3/tiledb-macos-x86_64-2.18.3-8f8b766.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
