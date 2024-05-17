/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import com.demandware.carbonj.service.engine.DataPoint;

/**
 * Representing "null" metric. Primary purpose is to avoid creating Optional instance for
 * every Metric instance we need to cache.
 */
public class NullMetric extends Metric
{
    public static final NullMetric METRIC_NULL = new NullMetric();

    private NullMetric()
    {
        super("NULL_METRIC", DataPoint.UNKNOWN_ID, null, null, null);
    }

    public static NullMetric getInstance()
    {
        return METRIC_NULL;
    }
}
