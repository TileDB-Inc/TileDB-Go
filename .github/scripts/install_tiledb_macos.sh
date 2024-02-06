set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.20.0-rc1/tiledb-macos-x86_64-2.20.0-rc1-9a3c2dc.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
