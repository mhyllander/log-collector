log-collector
=============

This is a log forwarding utility, similar to logstash-forwarder. Because
logstash-forwarder is too buggy I decided to roll my own. This collector uses
ZeroMQ to to send logs through a load balancing broker/queue and on to one or
more logstash indexers.

Features:

* Handles log file rotation. The old log file is finished before starting with
  the new one.
* Includes a multiline filter that can append continuation lines.
* Sends batches of log events to logstash, then waits for an ACK before it
  saves an updated state.
* Uses the Paranoid Pirate Pattern (ZeroMQ), modifed with a ping-pong heartbeat.
  Log collectors and logstash indexers can be added and removed dynamically
  without reconfiguring or restarting any other instances.
* The queue load balancer spreads batches over the available logstash indexers.

Log-collector is written in Ruby and is powered by the eventmachine,
eventmachine-tail and ffi-rzmq gems. Eventmachine-tail has been enhanced and
fixed a bit (custom version 0.6.4.1).

Currently log-collector used ZeroMQ 4.0.4 and ffi-rzmq 2.0.1. The version
bundled with logstash 1.4.1 is too old, therefore ffi-rzmq 2.0.1 needs to be
added to the JRuby that comes with logstash (see below).

A logcollector input is provided for logstash, which needs to be saved to the
lib/logstash/inputs directory.

Install latest ffi-rzmq gem in Logstash's JRuby bundle
------------------------------------------------------

```bash
cd logstash-1.4.1/vendor
GEM_HOME=$PWD/bundle/jruby/1.9 GEM_PATH=$PWD/bundle/jruby/1.9 java -jar jar/jruby-complete-1.7.11.jar -S gem install -v 2.0.1 ffi-rzmq
```

Create ZeroMQ deb package
-------------------------

```bash
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
```

(Got a strange error on one machine when running ldconfig, that some libraries were not symbolic links.
Had to fix that manually before running ldconfig.)

Create log-collector deb package
--------------------------------

```bash
cd log-collector/..
fpm -s dir -t deb -n log-collector -v 0.1.0 -a all -C log-collector --prefix opt/log-collector/ -p log-collector-VERSION_ARCH.deb log-collector bundle zmq-broker logstash-inputs
```
