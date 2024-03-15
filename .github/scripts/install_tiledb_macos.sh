set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/${CORE_VERSION}/tiledb-macos-x86_64-${CORE_VERSION}-${CORE_HASH}.tar.gz \
&& sudo tar -C /usr/local -xSf tiledb.tar.gz
