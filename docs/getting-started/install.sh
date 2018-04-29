#!/bin/sh -e

LIB=~/quantumdb
mkdir -p $LIB
cd $LIB
curl -# -O -L 'https://github.com/quantumdb/quantumdb/releases/download/quantumdb-0.3.0/quantumdb-cli-0.3.0.jar'

echo '#!/bin/sh
java -jar '$LIB'/quantumdb-cli-0.3.0.jar "$@"' > /usr/local/bin/quantumdb
chmod +x /usr/local/bin/quantumdb