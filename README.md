log-collector
=============

Install ZeroMQ
--------------

sudo apt-get install libtool autoconf automake uuid-dev build-essential
cd ~
wget http://download.zeromq.org/zeromq-4.0.4.tar.gz
tar zxvf zeromq-4.0.4.tar.gz && cd zeromq-4.0.4
./configure
make && sudo make install
sudo ldconfig
