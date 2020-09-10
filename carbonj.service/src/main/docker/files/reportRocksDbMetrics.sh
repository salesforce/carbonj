#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


pod=pod98
realm=${DW_GROUP_ID}
service=${SVC_PROP_APP_NAME}
instance=$( hostname )
serviceName=${DW_SVC_VERSION}

graphite_host=${DW_GRAPHITE_HOST}
graphite_port=2003

all=("30m2y" "5m7d"  "60s24h"  "index-id"  "index-name")

for db in "${all[@]}"
do
    log="/data/carbonj-data/${db}/LOG"
    metric_name="$pod.$realm.carbonj.$instance.${serviceName//./_}.rocksdb.${db}.compaction.bytes"
    time=$( date +%s )
    # Format date: 2017/11/24-12:22
    LAST_MIN=`date --date='-1 minute' "+%Y/%m/%d-%H:%M"`
    #bytes=`awk -v last_min="$LAST_MIN" 'BEGIN { x=0; } $1 ~"^"last_min && $6=="Compacted" { x+=$(NF-1); } END {  print x }' $log`
    bytes=`tail -n 10000 $log | stdbuf -o0 awk -v last_min="$LAST_MIN" 'BEGIN {x=0;} $1~"^"last_min && /Compacted/{ x+=$(NF-1); } END {  print x }'`
    line="$metric_name $bytes $time"
    echo "${line}"
    echo "${line}" | nc -u -v $graphite_host $graphite_port
done
exit $?
