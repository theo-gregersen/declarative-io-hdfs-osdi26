#!/bin/bash

mvn package -Pdist,native -DskipTests -Dtar \
    -Drequire.isal -Disal.lib=/usr/lib/ -Dbundle.isal \
    -Drequire.snappy -Dsnappy.lib=/usr/lib/x86_64-linux-gnu/ -Dbundle.snappy \
    -Drequire.openssl -Dopenssl.lib=/usr/lib/x86_64-linux-gnu/ -Dbundle.openssl

# hadoop checknative -a
