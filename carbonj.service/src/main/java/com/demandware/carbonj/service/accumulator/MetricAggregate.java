/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.google.common.base.Preconditions;

public class MetricAggregate
{
    final private String aggregateName;
    final private MetricAggregationMethod aggregationMethod;
    final private boolean dropOriginal;

    public MetricAggregate(String aggregateName, MetricAggregationMethod method, boolean dropOriginal)
    {
        this.aggregateName = Preconditions.checkNotNull( aggregateName );
        this.aggregationMethod = Preconditions.checkNotNull( method );
        this.dropOriginal = dropOriginal;
    }

    public String getAggregateName()
    {
        return aggregateName;
    }

    public MetricAggregationMethod getAggregationMethod()
    {
        return aggregationMethod;
    }

    public boolean isDropOriginal()
    {
        return dropOriginal;
    }
}
