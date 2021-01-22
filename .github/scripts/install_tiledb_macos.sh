set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.2.1/tiledb-macos-2.2.1-4744a3f-full.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz