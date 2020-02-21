#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

DIR=$( dirname "$0" )

if [ -f $DIR/config/service.env ]
then
       source $DIR/config/service.env
fi

POD_ID=$(</etc/podid)

VERSION=$(ls /build/carbonj/carbonj/carbonj*.jar | cut -d '/' -f 5 | cut -d '-' -f 3 | cut -d '.' -f 1,2,3)

LOG_DIR=/app_logs/carbonj

LOG_FILE=$LOG_DIR/carbonj.log

CONFIG_FILE=/etc/carbonj/application.yml

export AWS_CREDENTIAL_PROFILES_FILE=/etc/aws/credentials
export AWS_CONFIG_FILE=/etc/aws/config

# Build the command
COMMAND="${JAVA} -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR} -jar /build/carbonj/carbonj/carbonj-*.jar --logging.file=${LOG_FILE} --spring.config.location=${CONFIG_FILE}"

echo -e "Invoking carbonj\n" | tee -a $LOG_FILE
echo -e $COMMAND | tee -a $LOG_FILE
echo -e "\n" | tee -a $LOG_FILE
$COMMAND &
