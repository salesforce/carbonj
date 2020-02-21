/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * Immutable metric aggregation rules.
 */
class MetricAggregationRules
{
    private static final Logger log = LoggerFactory.getLogger( MetricAggregationRules.class );

    final private int revision;
    final private List<MetricAggregationRule> rules;

    /**
     * Initializes instance with rules loaded from file.
     */
    public MetricAggregationRules( int revision, List<MetricAggregationRule> rules)
    {
        this.revision = revision;
        this.rules = rules;
    }

    public int size()
    {
        return rules.size();
    }



    public boolean isEmpty()
    {
        return rules.isEmpty();
    }

    public List<MetricAggregationRule.Result> apply( String metricName )
    {
        List<MetricAggregationRule.Result> results = new ArrayList<>(  );

        for(MetricAggregationRule rule : rules)
        {
            MetricAggregationRule.Result result = rule.apply( metricName );
            if( result.ruleApplied() )
            {
                results.add( result );

                if( rule.isStopRule() )
                {
                    break;
                }
            }
        }

        return results;
    }

    public int getRevision()
    {
        return revision;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper( this )
                      .add( "revision", revision )
                      .add( "rules", rules )
                      .toString();
    }
}
