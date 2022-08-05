set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.10.4/tiledb-macos-x86_64-2.10.4-f2b5d11.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
