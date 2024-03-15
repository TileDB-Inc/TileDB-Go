set -e -x
source "$(dirname $0)/tiledb-version.sh"
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/${CORE_VERSION}/tiledb-linux-x86_64-${CORE_VERSION}-${CORE_HASH}.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
sudo ldconfig /usr/local/lib
