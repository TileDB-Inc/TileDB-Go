set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.18.5/tiledb-macos-x86_64-2.18.5-0466b2b.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
