/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.google.common.base.Preconditions;

class MetricAggregationPolicyProviderWithDb
    implements MetricAggregationPolicyProvider
{
    final private TimeSeriesStore db;
    final private MetricAggregationPolicySource policySource;

    public MetricAggregationPolicyProviderWithDb( TimeSeriesStore db, MetricAggregationPolicySource policySource )
    {
        this.db = Preconditions.checkNotNull( db );
        this.policySource = Preconditions.checkNotNull( policySource );
    }

    @Override
    public MetricAggregationPolicy metricAggregationPolicyFor( String name )
    {
        Metric m = db.getMetric( name );
        if( m == null ) // for new metric name - metric won't exist yet.
        {
            return policySource.policyForMetricName( name );
        }

        MetricAggregationPolicy p = m.getMetricAggregationPolicy();

        if( p == null || policySource.isObsolete( p ) )
        {
            p = m.setMetricAggregates( policySource.policyForMetricName( name ) );
        }

        return p;
    }


}
