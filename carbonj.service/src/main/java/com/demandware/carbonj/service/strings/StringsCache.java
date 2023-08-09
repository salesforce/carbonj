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
    private static final Logger log = LoggerFactory.getLogger( StringsCache.class );

    private static LoadingCache<String, State> cache;

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
                            .build( new CacheLoader<>()
                            {
                                @SuppressWarnings("NullableProblems")
                                @Override
                                public State load( String key ) {
                                    return new State(key);
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
            return null == cache ? key : cache.get( key ).key;
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( "key: " + key, e);
        }
    }

    public static State getState(String key) {
        try {
            return cache == null ? null : cache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException("key: " + key, e);
        }
    }

    public static void invalidateCache() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    @PreDestroy
    public void close()
    {
        // ensure that metrics are unregistered. Needed for unit tests.
        statsReporter.close();
    }

    public static class State {
        private final String key;
        private volatile Boolean isBlackListed;

        private State(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public Boolean getBlackListed() {
            return isBlackListed;
        }

        public void setBlackListed(Boolean blackListed) {
            isBlackListed = blackListed;
        }
    }
}
