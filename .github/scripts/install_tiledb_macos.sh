set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.3.4/tiledb-macos-x86_64-2.3.4-e19855e.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
