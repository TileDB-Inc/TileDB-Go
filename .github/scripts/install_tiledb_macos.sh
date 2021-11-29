set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.5.2/tiledb-macos-x86_64-2.5.2-f9c058f.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
