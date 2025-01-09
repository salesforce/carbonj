/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.engine.AggregationMethod;
import com.demandware.carbonj.service.engine.StorageAggregationRules;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import com.google.common.base.Preconditions;

/**
 * Factory for instances of StorageAggregationPolicy class.
 */
public class StorageAggregationPolicySource
{
    private static final Logger log = LoggerFactory.getLogger( StorageAggregationPolicySource.class );

    // reuse same instance across multiple metrics.
    private final CopyOnWriteArrayList<AggregationPolicy> policies = new CopyOnWriteArrayList<>(  );

    private final StorageAggregationRulesLoader rulesLoader;

    public StorageAggregationPolicySource( StorageAggregationRulesLoader rulesLoader)
    {
        this.rulesLoader = Preconditions.checkNotNull( rulesLoader );
    }


    public int currentConfigRevision()
    {
        return rulesLoader.getRules().getRevision();
    }

    public AggregationPolicy policyForMetricName(String name)
    {
        StorageAggregationRules rules = rulesLoader.getRules();
        AggregationMethod method = rules.apply( name );
        return getInstance( method, rules.getRevision() );
    }

    private AggregationPolicy getInstance( AggregationMethod method, int revision )
    {
        // it is not expensive to create a new instance. The cache is used to enable instance reuse -
        AggregationPolicy ap = new AggregationPolicy( method, revision, this );
        int i = policies.indexOf( ap );
        if( i >= 0 )
        {
            return policies.get( i );
        }
        else
        {
            if( policies.addIfAbsent( ap ) )
            {
                return ap;
            }
            else
            {
                return policies.get( policies.indexOf( ap ) );
            }
        }
    }

    // synchronized to prevent multiple instances of this method executing at the same time.
    public synchronized void cleanup()
    {
        log.info("checking for obsolete aggregation policies to remove from cache");
        List<AggregationPolicy> obsolete = policies.stream()
                                                   .filter(AggregationPolicy::configChanged)
                                                   .toList();
        // no need to keep policies that represent obsolete config.
        policies.removeAll( obsolete );
        log.info("purged obsolete aggregation policies from cache. Number of obsolete policies found: "
            + obsolete.size() + ", total number of policies after purge: " + policies.size());
    }
}
