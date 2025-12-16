#!/bin/bash

# install postgresql
wget https://ftp.postgresql.org/pub/source/v16.2/postgresql-16.2.tar.gz
tar xzf postgresql-16.2.tar.gz
cd postgresql-16.2
./configure --prefix=$HOME/pgsql
make -j$(nproc)
make install 
cd ..


# install libpqxx
wget https://github.com/jtv/libpqxx/releases/download/7.10.2/libpqxx-7.10.2.tar.gz
tar xzf libpqxx-7.10.2.tar.gz
cd libpqxx-7.10.2
mkdir build && cd build

cmake .. \
  -DCMAKE_INSTALL_PREFIX=$HOME/pgsql \
  -DBUILD_SHARED_LIBS=ON \
  -DPostgreSQL_INCLUDE_DIR=$HOME/pgsql/include \
  -DPostgreSQL_LIBRARY=$HOME/pgsql/lib/libpq.so \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON

make -j$(nproc)
make install

curl -O https://www.ivarch.com/programs/sources/pv-1.7.0.tar.bz2 #need vpn
tar -xjf pv-1.7.0.tar.bz2
cd pv-1.7.0
./configure --prefix=$HOME/local
make
make install
export PATH=$HOME/local/bin:$PATH