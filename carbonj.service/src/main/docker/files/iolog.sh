#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


#
# Salesforce
# Ben Susman and Dmitry Babenko, 2017
#

# pod id
pod=pod98

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
prefix=$pod.$groupId.$appName.$hostName.${serviceName//./_}.iostat

# cache host
grafServer=${DW_GRAPHITE_HOST}
grafPort=2003

# setup
BLOB=""
interval=60

# Get stats printout ( blocks for $interval )
stats=$( iostat -xd $interval 1 | tail -n +4 )
time=$( date +%s )

# Loop each entry
readarray -t statarr <<<"$stats"
for stat in "${statarr[@]}"
do

	device=$( echo $stat | awk '{print $1}' )

	r_iops=$( echo $stat | awk '{print $4}' )
	message="$prefix.$device.read.iops $r_iops $time"
	BLOB="$BLOB\n$message"

	w_iops=$( echo $stat | awk '{print $5}' )
	message="$prefix.$device.write.iops $w_iops $time"
	BLOB="$BLOB\n$message"

	r_kB=$( echo $stat | awk '{print $6}' )
	message="$prefix.$device.read.kB $r_kB $time"
	BLOB="$BLOB\n$message"

	w_kB=$( echo $stat | awk '{print $7}' )
	message="$prefix.$device.write.kB $w_kB $time"
	BLOB="$BLOB\n$message"

	await=$( echo $stat | awk '{print $10}' )
	message="$prefix.$device.await $await $time"
	BLOB="$BLOB\n$message"

	r_await=$( echo $stat | awk '{print $11}' )
	message="$prefix.$device.read.await $r_await $time"
	BLOB="$BLOB\n$message"

	w_await=$( echo $stat | awk '{print $12}' )
	message="$prefix.$device.write.await $w_await $time"
	BLOB="$BLOB\n$message"

	svc=$( echo $stat | awk '{print $13}' )
	message="$prefix.$device.svc $svc $time"
	BLOB="$BLOB\n$message"

done

# Print to server
BLOB="$BLOB\n"
echo -e "$BLOB"
echo -e "$BLOB" | nc -u -v $grafServer $grafPort
exit $?
