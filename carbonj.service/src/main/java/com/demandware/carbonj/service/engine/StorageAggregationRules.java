/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.MoreObjects;

/**
 * All aggregation rules.
 */
public class StorageAggregationRules
{
    private int revision;
    private List<StorageAggregationRule> rules = new ArrayList<>();

    /**
     * Initializes instance with rules.
     */
    public StorageAggregationRules(int revision, List<StorageAggregationRule> rules)
    {
        this.revision = revision;
        this.rules = rules;
    }

    public int size()
    {
        return rules.size();
    }

    public int getRevision()
    {
        return revision;
    }

    public AggregationMethod apply( String metricName )
    {
        AggregationMethod method = null;

        for(StorageAggregationRule rule : rules)
        {
            method = rule.apply( metricName );
            if( method != null )
            {
                break;
            }
        }

        return method != null ? method : AggregationMethod.AVG;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper( this )
                      .add( "revision", revision )
                      .toString();
    }
}
