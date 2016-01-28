#!/bin/bash

VERSION="$1"
ARCH=amd64

if [[ $VERSION ]]
then

  find . -type f -name '*~' -print0 | xargs -0r rm

  fpm -s dir -t deb -n log-collector-jruby -v "$VERSION" -a "$ARCH" -d zeromq --prefix opt/log-collector/ -p log-collector-jruby-VERSION_ARCH.deb README.md bin log-collector bundle zmq-broker logstash-inputs

  FILE="log-collector-jruby-${VERSION}_${ARCH}.deb"

  [[ -f $FILE ]] && sha256sum "$FILE" > "$FILE".sha

fi
