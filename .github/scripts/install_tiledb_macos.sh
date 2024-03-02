set -e -x
source "$(dirname $0)/tiledb-version.sh"
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/${TILEDB_VERSION}/tiledb-macos-x86_64-${TILEDB_VERSION}-${COMMIT_ID}.tar.gz \
&& sudo tar -C /usr/local -xSf tiledb.tar.gz
# Trigger a run
