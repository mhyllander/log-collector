log-collector
=============

Create ZeroMQ deb package
-------------------------

sudo apt-get install libtool autoconf automake uuid-dev build-essential
sudo gem install fpm
cd ~
wget http://download.zeromq.org/zeromq-4.0.4.tar.gz
tar zxvf zeromq-4.0.4.tar.gz && cd zeromq-4.0.4
./configure
make
make install DESTDIR=/tmp/zmqinst
pushd /tmp/zmqinst/usr/local/lib
for L in libzmq.so libzmq.so.3; do rm $L; ln -s libzmq.so.3.1.0 $L; done
popd
fpm -s dir -t deb -n zeromq -v 4.0.4 -C /tmp/zmqinst -p zeromq-VERSION_ARCH.deb usr/local

(Got a strange error on one machine when running ldconfig, that some libraries were not symbolic links.
Had to fix that manually before running ldconfig.)

Create log-collector deb package
--------------------------------

cd log-collector/..
fpm -s dir -t deb -n log-collector -v 0.1.0 -a all -C log-collector --prefix opt/log-collector/ -p log-collector-VERSION_ARCH.deb log-collector bundle zmq-broker


Install latest ffi-rzmq gem in Logstash's JRuby bundle
------------------------------------------------------

cd logstash-1.4.1
GEM_HOME=$PWD/vendor/bundle/jruby/1.9 GEM_PATH=$PWD/vendor/bundle/jruby/1.9 java -jar jar/jruby-complete-1.7.11.jar -S gem install -v 2.0.1 ffi-rzmq
