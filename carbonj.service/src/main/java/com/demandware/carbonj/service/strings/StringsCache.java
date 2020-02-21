/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.strings;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.CacheStatsReporter;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringsCache implements StatsAware
{
    private static Logger log = LoggerFactory.getLogger( StringsCache.class );

    private static LoadingCache<String, String> cache;

    final private CacheStatsReporter statsReporter;

    // Spring singleton bean.
    public StringsCache(MetricRegistry metricRegistry, int initialCacheSize, int maxCacheSize, int expireAfterLastAccessInMinutes, int concurrencyLevel)
    {
        log.info( String.format( "initialCacheSize: %s, maxSize: %s, expireAfterLastAccessInMinutes: %s, concurrencyLevel: %s",
            initialCacheSize, maxCacheSize, expireAfterLastAccessInMinutes, concurrencyLevel));

        if( maxCacheSize > 0 )
        {
            cache = CacheBuilder.newBuilder()
                            .initialCapacity( initialCacheSize )
                            .maximumSize( maxCacheSize )
                            .concurrencyLevel( concurrencyLevel )
                            .expireAfterAccess( expireAfterLastAccessInMinutes, TimeUnit.MINUTES )
                            .recordStats()
                            .build( new CacheLoader<String, String>()
                            {
                                @Override
                                public String load( String key )
                                                throws Exception
                                {
                                    return key;
                                }
                            } );
            statsReporter = new CacheStatsReporter( metricRegistry,"StringsCache", maxCacheSize, cache );
        }
        else
        {
            cache = null;
            statsReporter = null;
        }
    }

    @Override
    public void dumpStats()
    {
        statsReporter.dumpStats();
    }

    // static to avoid reference overhead at instance level in objects like Points or Metrics.
    public static String get(String key)
    {
        try
        {
            return null == cache ? key : cache.get( key );
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( "key: " + key, e);
        }
    }

    @PreDestroy
    public void close()
    {
        // ensure that metrics are unregistered. Needed for unit tests.
        statsReporter.close();
    }

}
