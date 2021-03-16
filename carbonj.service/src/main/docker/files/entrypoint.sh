#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


###################
### from config.sh
###################

if [ -d /conf ]; then
    # override each config file coming from /conf, ignore others
    pushd . > /dev/null
    cd /conf
    for f in * ; do
        ln -sf /conf/$f $SERVICEDIR/config/$f;
    done
	popd > /dev/null
fi

# if service.properties are provided, remove jar-supplied application.yml since it is not in fact overridden by
# service.properties. If application.yml is present, service.properties are going to be ignored.
if [ -f /app/config/service.properties ]; then
#  rm  /app/application.yml
  mv /app/config/service.properties /app/config/application.properties
fi

if [ -d /data ]; then
	rm -Rf $SERVICEDIR/work
	ln -s /data $SERVICEDIR/work
	mkdir $SERVICEDIR/work/carbonj-data
	mkdir $SERVICEDIR/work/carbonj-tmp
	mkdir $SERVICEDIR/work/carbonj-staging
	mkdir $SERVICEDIR/work/log
	ln -s $SERVICEDIR/work/log $SERVICEDIR/log
fi

#set default spring profile if none is given. exclusively used for properties overrides file
if [ -z $spring_profiles_active ]
then
  spring_profiles_active='dev'
fi

# you can specify any spring config property (i.e. jetty.port=2001) by supplying
# 'application-$spring_profiles_active.properties' which is a file that is automatically consumed by spring.
for SVC_PROP in `compgen -A variable | grep "^SVC_PROP_"` ; do
  var_replace_svc_prop="${SVC_PROP/SVC_PROP_/}"
	var_underscore_replaced="${var_replace_svc_prop//_/.}"
	var_space_replaced="${var_underscore_replaced// /}"
	var_lowercase=${var_space_replaced,,}
	printf '%s=%s\n' "$var_lowercase" "${!SVC_PROP}" >> $SERVICEDIR/config/overrides.properties
done

#########################
#########################
#########################

JAVA_OPTS=''

SERVICEARGS="/app/config/service.args"
while IFS= read -r LINE
do
  LINE_NO_WHITESPACE="$(echo -e "${LINE}" | tr -d '[:space:]')"
  if [ ! -z "$LINE_NO_WHITESPACE" ]
  then
    if [[ ! $LINE_NO_WHITESPACE =~ ^#.*  ]]
    then
      JAVA_OPTS="${JAVA_OPTS} ${LINE_NO_WHITESPACE}"
    fi
  fi
done < "$SERVICEARGS"

if [ ! -z ${XMX_SIZE+x} ]
then
  JAVA_OPTS="${JAVA_OPTS} -Xmx${XMX_SIZE} "
fi
if [ ! -z ${XMS_SIZE+x} ]
then
  JAVA_OPTS="${JAVA_OPTS} -Xms${XMS_SIZE} "
fi
JAVA_OPTS=`eval echo "${JAVA_OPTS}"`
echo $JAVA_OPTS $JAVA_OPTS_OVERRIDE

env >> /etc/environment

# no logs on disk
crontab -l | { cat; echo "* */6 * * * /app/bin/logCleanup.sh ${SERVICEDIR}/log 7 >/dev/null 2>&1"; } | crontab -
crontab -l | { cat; echo "*/1 * * * * /app/bin/fdlog.sh"; } | crontab -
crontab -l | { cat; echo "*/1 * * * * /app/bin/iolog.sh"; } | crontab -
crontab -l | { cat; echo "*/1 * * * * /app/bin/reportGcMetrics.sh"; } | crontab -
crontab -l | { cat; echo "*/1 * * * * /bin/bash -c '/usr/bin/perl /app/bin/requestlog-stats.pl'"; } | crontab -
crontab -l | { cat; echo "*/1 * * * * /app/bin/disklog.sh"; } | crontab -
if [[ $ROCKSDB_REPORTING_ENABLED = 1 ]]; then
    crontab -l | { cat; echo "*/1 * * * * /app/bin/reportRocksDbMetrics.sh"; } | crontab -
fi
crontab -l

echo "Running cron"
/usr/sbin/crond
echo "Running service"
cd /app
exec java $JAVA_OPTS $JAVA_OPTS_OVERRIDE -Dlogback.debug=true -cp /app:/app/lib/* com.demandware.carbonj.service.engine.CarbonJServiceMain --spring.config.location=classpath:/application.yml,classpath:/,classpath:/config/,classpath:/config/application.properties,classpath:/config/overrides.properties
