set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.2.9/tiledb-macos-2.2.9-dc3bb54-full.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
