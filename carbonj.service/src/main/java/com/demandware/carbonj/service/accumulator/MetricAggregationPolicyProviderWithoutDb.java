/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.google.common.base.Preconditions;

class MetricAggregationPolicyProviderWithoutDb
    implements MetricAggregationPolicyProvider
{
    final private MetricAggregationPolicySource policySource;

    public MetricAggregationPolicyProviderWithoutDb( MetricAggregationPolicySource policySource )
    {
        this.policySource = Preconditions.checkNotNull( policySource );
    }

    @Override
    public MetricAggregationPolicy metricAggregationPolicyFor( String name )
    {
        // for now - now cache.
        return policySource.policyForMetricName( name );
    }


}
