#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

if [ -d /opt/carbonj ]; then
  HOME_DIR=/opt/carbonj
else
  HOME_DIR=$( pwd )
fi
echo $HOME_DIR

if [ -z ${JAVA+x} ]; then
    JAVA=`type -p java`
fi

if [ -z ${LOG_DIR+x} ]; then
    LOG_DIR=/var/log/carbonj
fi

LOG_FILE=$LOG_DIR/carbonj.log

CLASSPATH=$DIR

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
COMMAND="${JAVA} -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR} \
--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED -jar ${HOME_DIR}/lib/carbonj*.jar \
--logging.file.name=${LOG_FILE} --spring.config.location=${CONFIG_FILE}"

echo -e "Invoking CarbonJ\n" | tee -a $LOG_FILE
echo -e $COMMAND | tee -a $LOG_FILE
echo -e "\n" | tee -a $LOG_FILE
$COMMAND &
