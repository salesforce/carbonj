/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.demandware.carbonj.service.accumulator.MetricAggregationPolicy;
import com.google.common.base.Preconditions;

import com.demandware.carbonj.service.strings.StringsCache;

public class MsgPackMetric
{
    final public String path;
    final public boolean isLeaf;

    public MsgPackMetric( Metric metric )
    {
        this.path = metric.name;
        this.isLeaf = metric.isLeaf();
    }
}
