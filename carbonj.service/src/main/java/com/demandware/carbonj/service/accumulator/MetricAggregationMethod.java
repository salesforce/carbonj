/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

/**
 * Metric Aggregation Method. Defines rule for mapping points from multiple metric names into a new metric name.
 *
 * This is different from Storage Aggregation Method.
 */
public enum MetricAggregationMethod
{
    CUSTOM1, // uses avg or sum based on metric name suffix
    AVG,
    SUM,
    LATENCY;
}

