set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.17.0/tiledb-macos-x86_64-2.17.0-93c173d.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
