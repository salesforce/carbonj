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
prefix=$pod.$groupId.$appName.$hostName.${serviceName//./_}.file_descriptors

# cache host
grafServer=${DW_GRAPHITE_HOST}
grafPort=2003

# Get stats printout ( blocks for $interval )
count=$( lsof -c java | wc -l )
time=$( date +%s )

# Print to server
echo -e "$prefix.count $count $time"
echo -e "$prefix.count $count $time" | nc -u -v $grafServer $grafPort
exit $?
