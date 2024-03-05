set -e -x
source "$(dirname $0)/tiledb-version.sh"
dl_dir="/tmp/download"
mkdir -m 0777 -p "$dl_dir"
curl --location -o "${dl_dir}/tiledb.tar.gz" \
    https://github.com/dudoslav/TileDB/releases/download/t05/tiledb-macos-x86_64-2.21.0.tar.gz
sudo tar -tvf "${dl_dir}/tiledb.tar.gz"
sudo tar -C /usr/local -mxf "${dl_dir}/tiledb.tar.gz"
