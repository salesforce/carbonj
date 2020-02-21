/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import java.util.List;

public class MetricAggregationPolicy
{
    private final int revision;

    private final List<MetricAggregate> aggregates;

    public MetricAggregationPolicy(int revision, List<MetricAggregate> aggregates)
    {
        this.revision = revision;
        this.aggregates = aggregates;
    }

    public int getRevision()
    {
        return revision;
    }

    public List<MetricAggregate> getAggregates()
    {
        return aggregates;
    }
}
