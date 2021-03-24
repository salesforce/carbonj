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

if [ -z ${JAVA+x} ] && [ -d /build/OpenJDK/1.8.0.172_1/ ]; then
    JAVA=/build/OpenJDK/1.8.0.172_1/jdk64/bin/java
else
    JAVA=`type -p java`
fi

if [ -z ${LOG_DIR+x} ]; then
    LOG_DIR=/app_logs/carbonj
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

if [ -z ${CONF_DIR+x} ]; then
    CONF_DIR=/etc/carbonj
fi

# Build the command
COMMAND="${JAVA} -Xms4g -Xmx4g -XX:NewSize=600m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR} -XX:OnOutOfMemoryError=/build/carbonj/carbonj/bin/onOutOfMemoryError -Dfile.encoding=UTF-8 -Duser.timezone=GMT -Duser.language=en -Duser.country=US -Dio.netty.noUnsafe=true -jar ${HOME_DIR}/lib/carbonj*.jar --logging.path=${LOG_DIR} --spring.config.location=${CONF_DIR}/application.yml"

echo -e "Invoking CarbonJ\n" | tee -a $LOG_FILE
echo -e $COMMAND | tee -a $LOG_FILE
echo -e "\n" | tee -a $LOG_FILE
$COMMAND &
