/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

public class CacheStatsReporter implements StatsAware
{
    private static final Logger log = LoggerFactory.getLogger( CacheStatsReporter.class );

    // keep track of created names to unregister as part of close.
    // This is relevant for unit tests because attempt to register existing metric name will result in exception.
    private final MetricRegistry metricRegistry;
    final private List<String> metricNames = new ArrayList<>(  );
    final private String name;
    final private long maxSize;
    final private LoadingCache cache;
    private volatile CacheStats statsSnapshot;



    public CacheStatsReporter( MetricRegistry metricRegistry, String name, long maxSize, LoadingCache cache)
    {
        this.metricRegistry = metricRegistry;
        this.name = name;
        this.maxSize = maxSize;
        this.cache = cache;
        this.statsSnapshot = cache != null ? cache.stats() : null;

        register( "maxSize", ( ) -> this.maxSize );
        register( "size", () -> cache != null ? this.cache.size() : 0);
        if( statsSnapshot != null )
        {
            register( "hitCount", () -> this.statsSnapshot.hitCount() );
            register( "missCount", () -> this.statsSnapshot.missCount() );
            register( "requestCount", () -> this.statsSnapshot.requestCount() );
        }
    }

    private void register(String gaugeName, Gauge<Long> gauge)
    {
        String metricName = MetricRegistry.name( "cache", name, gaugeName );
        try
        {
            metricRegistry.register( metricName, gauge );
        }
        catch(IllegalArgumentException e)
        {
            // this can happen in unit tests if previous tests didn't tear down Spring context properly.
            log.warn( "attempted create metric name that is already exists. ", e );
            metricRegistry.remove( metricName );
            metricRegistry.register( metricName, gauge );
        }
        metricNames.add( metricName );
    }

    @Override
    public void dumpStats()
    {
        if( cache != null )
        {
            this.statsSnapshot = cache.stats();
            log.info( String.format( "Cache [%s]. max cache size: %s, current size: %s, stats: %s",
                name, maxSize, cache.size(), statsSnapshot.toString() ) );
        }
        else
        {
            log.info( String.format( "cache [%s] is not configured.", name) );
        }
    }

    public void close()
    {
        metricNames.forEach( m ->  metricRegistry.remove(m));
    }
}
