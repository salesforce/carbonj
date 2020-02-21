/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

/**
 * Returns list of aggregates for given metric name.
 */
public interface MetricAggregationPolicyProvider
{
    MetricAggregationPolicy metricAggregationPolicyFor( String name );
}
