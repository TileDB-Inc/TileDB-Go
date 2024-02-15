set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.20.1/tiledb-macos-x86_64-2.20.1-249c024.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
