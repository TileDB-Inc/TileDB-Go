set -e -x
# TODO: Revert change to use fork release from shaunrd0/TileDB.
curl --location -o tiledb.tar.gz https://github.com/shaunrd0/TileDB/releases/download/${CORE_VERSION}/tiledb-macos-arm64-${CORE_VERSION}-${CORE_HASH}.tar.gz
echo "777be952c51e7eaba95029e8d439d72cd008724f34c3bd9f741a51cc57663a2c  tiledb.tar.gz" >> checksums.txt
shasum -a 256 -c checksums.txt
rm checksums.txt
sudo tar -C /usr/local -xSf tiledb.tar.gz
