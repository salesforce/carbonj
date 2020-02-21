#!/usr/bin/env bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


while [ /bin/true ] ; do
    tail -f /root/.yjp/log/*
    sleep 1s
done