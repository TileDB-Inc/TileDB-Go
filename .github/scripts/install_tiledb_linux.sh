set -e -x
# TODO: Revert change to use fork release from shaunrd0/TileDB.
# https://linear.app/tiledb/issue/CLOUD-1043/revert-patched-core-version-used-in-tiledb-server
curl --location -o tiledb.tar.gz https://github.com/shaunrd0/TileDB/releases/download/${CORE_VERSION}/tiledb-linux-x86_64-${CORE_VERSION}-${CORE_HASH}.tar.gz
echo "3ee69238b3e91dbe5c3b935bd0617ba3c3a5c03559815106360c2fdab5618e94 tiledb.tar.gz" >> checksums.txt
sha256sum -c checksums.txt
rm checksums.txt

sudo tar -C /usr/local -xf tiledb.tar.gz
sudo ldconfig /usr/local/lib
