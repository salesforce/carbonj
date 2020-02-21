#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


#
# setup variables
#
SVCHOME=/app/

NOW=$(date +%s)

# in a docker container pod is assumed to be 1
PID=`pidof java`

DUMP_FILE=${SVCHOME}/work/heapdump.hprof
DUMP_NAME=dump-$NOW
DUMP_DIR=$SVCHOME/work/$DUMP_NAME
TARBALL=/${SVCHOME}/work/${DUMP_NAME}.tar.gz

mkdir -p $DUMP_DIR

mv $DUMP_FILE $DUMP_DIR || /bin/true

echo "Collecting 3 heap dumps and stack traces"
if [ ! -z $PID ] ;then
    for N in 1 2 3
    do
        echo "Collecting heap dump #$N"
        $JAVA_HOME/bin/jmap -histo $PID > $DUMP_DIR/jmap_${PID}_$N.txt   2>&1
        echo "Collecting stack trace #$N"
        $JAVA_HOME/bin/jstack $PID      > $DUMP_DIR/jstack_${PID}_$N.txt 2>&1
    done
fi

(cd $DUMP_DIR/..; tar czf $TARBALL $DUMP_NAME)
rm -r $DUMP_DIR
chmod 644 $TARBALL

# kill service after OOM to take it offline. the useful lifecycle of a Java process has ended here.
if [ ! -z $PID ] ;then
    echo "Killing Java process..."
    kill -9 $PID
fi
