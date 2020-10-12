#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

DIR=$( pwd )
echo $DIR

if [ -f $DIR/config/service.env ]
then
       source $DIR/config/service.env
fi

if [ -z ${JAVA+x} ]; then
    JAVA=`type -p java`
fi

if [ -z ${LOG_DIR+x} ]; then
    LOG_DIR=/app_logs/carbonj
fi

LOG_FILE=$LOG_DIR/carbonj.log

if [ -d /build/carbonj/carbonj/ ]; then
  CLASSPATH=/build/carbonj/carbonj
else
  CLASSPATH=$DIR
fi

if [ -z ${CONFIG_LOCATION+x} ]; then
  CONFIG_LOCATION=classpath:/application.yml,classpath:/,/etc/carbonj/application.yml,classpath:/config/,classpath:/config/application.properties,classpath:/config/overrides.properties
fi

if [ -z ${AWS_CREDENTIAL_PROFILES_FILE+x} ]; then
  export AWS_CREDENTIAL_PROFILES_FILE=/etc/aws/credentials
fi

if [ -z ${AWS_CONFIG_FILE+x} ]; then
  export AWS_CONFIG_FILE=/etc/aws/config
fi

# Build the command
COMMAND="${JAVA} -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR} -cp ${CLASSPATH}:${CLASSPATH}/* com.demandware.carbonj.service.engine.CarbonJServiceMain --logging.file=${LOG_FILE} --spring.config.location=${CONFIG_LOCATION}"

echo -e "Invoking carbonj\n" | tee -a $LOG_FILE
echo -e $COMMAND | tee -a $LOG_FILE
echo -e "\n" | tee -a $LOG_FILE
$COMMAND &
