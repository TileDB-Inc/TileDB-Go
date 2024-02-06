set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.20.0-rc2/tiledb-macos-x86_64-2.20.0-rc2-40552aa.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
