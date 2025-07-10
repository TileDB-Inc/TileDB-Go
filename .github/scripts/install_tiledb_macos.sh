set -e -x
# TODO: Revert change to use fork release from shaunrd0/TileDB.
curl --location -o tiledb.tar.gz https://github.com/shaunrd0/TileDB/releases/download/${CORE_VERSION}/tiledb-macos-arm64-${CORE_VERSION}-${CORE_HASH}.tar.gz \
&& sudo tar -C /usr/local -xSf tiledb.tar.gz
