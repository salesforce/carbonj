#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


logDir=$1
days=$2

# Delete older then X days
find $logDir -maxdepth 1 -type f -mtime +$days -print0 2> /dev/null | xargs -0 rm &> /dev/null

# Truncate larger then X bytes
find $logDir -maxdepth 1 -type f -size +1024M -print0 2> /dev/null | xargs -0 truncate --size=0 &> /dev/null

