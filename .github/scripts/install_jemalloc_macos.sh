set -e -x
wget https://github.com/jemalloc/jemalloc/archive/refs/tags/5.2.1.zip
unzip 5.2.1.zip
rm -rf unzip 5.2.1.zip
cd jemalloc-5.2.1
brew install autoconf automake libtool
./autogen.sh
make install