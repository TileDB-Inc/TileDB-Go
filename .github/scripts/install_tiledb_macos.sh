set -e -x
source "$(dirname $0)/tiledb-version.sh"
dl_dir="/tmp/download"
mkdir -m 0777 -p "$dl_dir"
curl --location -o "${dl_dir}/tiledb.tar.gz" \
    https://github.com/TileDB-Inc/TileDB/releases/download/${TILEDB_VERSION}/tiledb-macos-x86_64-${TILEDB_VERSION}-${COMMIT_ID}.tar.gz \
&& sudo gtar -C /usr/local -xSf "${dl_dir}/tiledb.tar.gz"
