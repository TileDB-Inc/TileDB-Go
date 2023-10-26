set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.17.3/tiledb-macos-x86_64-2.17.3-0c2de58.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
