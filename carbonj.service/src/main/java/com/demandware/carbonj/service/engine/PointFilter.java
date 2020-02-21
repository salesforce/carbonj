/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.python.google.common.base.Preconditions;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.index.NameUtils;
import com.demandware.carbonj.service.db.util.CacheStatsReporter;
import com.demandware.carbonj.service.db.util.Quota;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.db.util.SystemTime;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Filter points based on metric name length and ts.
 */
public class PointFilter implements StatsAware
{
    private static final Logger log = LoggerFactory.getLogger(PointFilter.class);

    private final int maxLen;

    private final int maxAge;

    private final int maxFutureAge;

    private final Meter maxLenExceededCount;

    private final Meter maxAgeExceededCount;

    private final Meter maxFutureAgeExceededCount;

    private final Meter tsDupCount;

    private final NameUtils nameUtils;

    private final Quota quota;

    /**
     * Keeps track of the last received point time for a given metric name.
     */
    private final LoadingCache<String, AtomicInteger> dupPointCache;
    private final CacheStatsReporter cacheStatsReporter;

    public PointFilter(MetricRegistry metricRegistry, String name, int maxLen, int maxAge, int maxFutureAge, NameUtils nameUtils,
                       int dupPointCacheMaxSize, int dupPointCacheExpireInMin, Quota q )
    {
        this.maxLen = maxLen;
        this.maxAge = maxAge;
        this.maxFutureAge = maxFutureAge;
        this.nameUtils = Preconditions.checkNotNull( nameUtils );
        this.dupPointCache = makeLastSeenTsMap( dupPointCacheMaxSize, dupPointCacheExpireInMin, TimeUnit.MINUTES );
        this.cacheStatsReporter = new CacheStatsReporter( metricRegistry,name + ".DuplicatePointCache", dupPointCacheMaxSize, dupPointCache );
        this.quota = q;

        this.maxLenExceededCount = metricRegistry.meter(
                MetricRegistry.name( "pointFilter", "maxLenExceededCount" ) );

        this.maxAgeExceededCount = metricRegistry.meter(
                MetricRegistry.name( "pointFilter", "maxAgeExceededCount" ) );

        this.maxFutureAgeExceededCount = metricRegistry.meter(
                MetricRegistry.name( "pointFilter", "maxFutureAgeExceededCount" ) );

        this.tsDupCount = metricRegistry.meter(
                MetricRegistry.name( "pointFilter", "tsDupDrops" ) );

    }

    private static LoadingCache<String, AtomicInteger> makeLastSeenTsMap( int maxSize, int expireAfterWrite, TimeUnit timeUnit)
    {
        if( maxSize > 0 )
        {
            log.info( String.format( "using map with maxSize {%s}", maxSize) );
            return CacheBuilder.newBuilder()
                               .maximumSize( maxSize )
                               .concurrencyLevel( 8 )
                               .recordStats()
                               .expireAfterAccess( expireAfterWrite, timeUnit )
                               .build( new CacheLoader<String, AtomicInteger>()
                               {
                                   public AtomicInteger load( String key )
                                   {
                                       return new AtomicInteger(0);
                                   }
                               } );
        }
        else
        {
            log.info( String.format( "check for duplicate timestamps is disabled because map  maxSize {%s}", maxSize) );
            return null;
        }
    }

    public void reset()
    {
        if( dupPointCache != null )
        {
            this.dupPointCache.invalidateAll();
        }
    }

    @Override
    public void dumpStats()
    {
        cacheStatsReporter.dumpStats();
    }

    public boolean accept( DataPoint p )
    {
        if ( !nameUtils.isValid( p.name ) )
        {
            // logging is done internally by nameUtils.
            return false;
        }

        if ( maxLen > 0 && p.name != null && p.name.length() > maxLen )
        {
            maxLenExceededCount.mark();
            if( quota.allow() )
            {
                log.warn( String.format( "point name is too long. Dropped point: [%s]", p) );
            }
            return false;
        }

        int now = SystemTime.nowEpochSecond();

        if ( maxAge > 0 && p.ts + maxAge < now )
        {
            maxAgeExceededCount.mark();
            if( quota.allow() )
            {
                log.warn( String.format( "point ts is too far in the past. Dropped point: [%s]", p) );
            }
            return false;
        }

        if ( maxFutureAge > 0 && p.ts > now + maxFutureAge )
        {
            maxFutureAgeExceededCount.mark();
            if( quota.allow() )
            {
                log.warn( String.format( "point ts is too far in the future. Dropped point: [%s]", p) );
            }
            return false;
        }

        if ( isDuplicateTs( p ) )
        {
            tsDupCount.mark();
            if( quota.allow() )
            {
                log.warn( String.format( "received multiple points within 60s interval. Dropped point: [%s]", p) );
            }
            return false;
        }

        return true;
    }

    private boolean isDuplicateTs( DataPoint p )
    {
        if( dupPointCache == null )
        {
            return false;
        }

        try
        {
            AtomicInteger lastSeenTs = dupPointCache.get( p.name );
            int pointIntervalTs = p.intervalValue( DataPoint.INPUT_POINT_PRECISION );
            final int maxAttempts = 10;
            int attemptsLeft = maxAttempts; // to avoid infinite loop
            while( attemptsLeft > 0 )
            {
                int lastSeenInterval = lastSeenTs.get();
                if ( lastSeenInterval == pointIntervalTs )
                {
                    return true;
                }

                if ( lastSeenTs.compareAndSet( lastSeenInterval, pointIntervalTs ) )
                {
                    break;
                }
                attemptsLeft--;
            }

            if( attemptsLeft == 0 )
            {
                log.warn( String.format( "Failed to check for duplicate point ts after %s attempts.", maxAttempts) );
            }
            else if( attemptsLeft < maxAttempts )
            {
                log.warn( String.format("Had to make %s attempts to update last seen ts for point %s", maxAttempts - attemptsLeft, p));
            }
        }
        catch ( ExecutionException e )
        {
            log.error( "Failed to check for duplicate point", e );
        }
        return false;
    }

    @PreDestroy
    public void close()
    {
        // ensure that metrics are unregistered. Needed for unit tests.
        this.cacheStatsReporter.close();
    }
}
