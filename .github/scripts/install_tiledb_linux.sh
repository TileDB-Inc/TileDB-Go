set -e -x
curl --location -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.11.0/tiledb-linux-x86_64-2.11.0-34e5dbc.tar.gz \
&& sudo tar -C /usr/local -xf tiledb.tar.gz
sudo ldconfig /usr/local/lib
