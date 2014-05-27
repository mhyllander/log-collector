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

Got a strange error on one machine when running ldconfig, that some libraries were not symbolic links.
Had to fix that manually before running ldconfig.


Install latest ffi-rzmq gem in Logstash's JRuby bundle
------------------------------------------------------

cd logstash-1.4.1
GEM_HOME=$PWD/vendor/bundle/jruby/1.9 GEM_PATH=$PWD/vendor/bundle/jruby/1.9 java -jar jar/jruby-complete-1.7.11.jar -S gem install -v 2.0.1 ffi-rzmq
