#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


# pod id
pod=${DW_POD_ID}

# group id
groupId=${DW_GROUP_ID}

# service name
serviceName=${DW_SVC_VERSION}

# app name
appName=${SVC_PROP_APP_NAME}

# host name
hostName=$( hostname )

# namespace
# namespace prefix
prefix=$pod.$groupId.$appName.$hostName.${serviceName//./_}.disk.used

# cache host
grafServer=${DW_GRAPHITE_HOST}
grafPort=2003

# Get stats printout ( blocks for $interval )
stats=$(df -T /data | grep data)
time=$( date +%s )

used=$( echo ${stats} | awk '{print $4}' )
message="$prefix.bytes $used $time"
echo ${message}
echo ${message} | nc -u -v ${grafServer} ${grafPort}

percent=$( echo ${stats} | awk '{print $6}' | tr % " ")
message="$prefix.percent $percent $time"
echo ${message}
echo ${message} | nc -u -v ${grafServer} ${grafPort}

exit $?
