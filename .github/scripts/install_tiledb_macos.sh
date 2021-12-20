set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.5.3/tiledb-macos-x86_64-2.5.3-dd6a41b.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
