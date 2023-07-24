/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

/**
 * Factory for instances of MetricAggregationPolicy class.
 */
class MetricAggregationPolicySource
{
    private MetricAggregationRulesLoader rulesLoader;
    private static final Logger log = LoggerFactory.getLogger( MetricAggregationPolicySource.class );

    public MetricAggregationPolicySource( MetricAggregationRulesLoader rulesLoader)
    {
        this.rulesLoader = Preconditions.checkNotNull( rulesLoader );
    }

    public boolean isObsolete( MetricAggregationPolicy p)
    {
        return p.getRevision() < currentConfigRevision();
    }

    public int currentConfigRevision()
    {
        return rulesLoader.getRules().getRevision();
    }

    public MetricAggregationPolicy policyForMetricName( String name)
    {
        MetricAggregationRules rules = rulesLoader.getRules();
        List<MetricAggregationRule.Result> results = rules.apply( name );
        log.info("Print the aggregate name: " + name);
        return getInstance( results, rules.getRevision() );
    }

    private MetricAggregationPolicy getInstance( List<MetricAggregationRule.Result> results, int revision )
    {
        List<MetricAggregate> aggregates = results.stream()
                                                  .map( r ->  new MetricAggregate( r.getAggregateName(), r.getMethod(), r.isDropOriginal() ))
                                                  .collect( Collectors.toList() );
        log.info("Get the list of aggregates from the metric aggregation policy: " + aggregates.size());
        return new MetricAggregationPolicy( revision, aggregates );
    }
}
