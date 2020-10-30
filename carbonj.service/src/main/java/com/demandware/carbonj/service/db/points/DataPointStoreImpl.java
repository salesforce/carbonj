/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.*;
import com.demandware.carbonj.service.db.util.CacheStatsReporter;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

class DataPointStoreImpl
    implements DataPointStore
{
    private static Logger log = LoggerFactory.getLogger( DataPointStoreImpl.class );

    private DataPointArchiveFactory dbFactory;

    private DatabaseMetrics dbMetrics;

    private DataPointStagingStore stagingStore;

    private boolean updateLowerResolutionArchives;

    private TimeSource timeSource = TimeSource.defaultTimeSource();

    private CacheStatsReporter seriesCacheStatsReporter;
    private LoadingCache<SeriesCacheKey, Series> seriesCache;

    private final QueryCachePolicy queryCachePolicy;

    private final Predicate<String> metricNamePresent;

    private static class SeriesCacheKey
    {
        final Metric m;

        final int from;

        final int until;

        final int now;

        public SeriesCacheKey( Metric m, int from, int until, int now )
        {
            this.m = m;
            this.from = from;
            this.until = until;
            this.now = now;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( !( o instanceof SeriesCacheKey ) )
            {
                return false;
            }

            SeriesCacheKey that = (SeriesCacheKey) o;

            if ( from != that.from )
            {
                return false;
            }
            if ( until != that.until )
            {
                return false;
            }
            if ( now != that.now )
            {
                return false;
            }
            return m.equals( that.m );

        }

        @Override
        public int hashCode()
        {
            int result = m.hashCode();
            result = 31 * result + from;
            result = 31 * result + until;
            result = 31 * result + now;
            return result;
        }
    }

    DataPointStoreImpl(MetricRegistry metricRegistry, DataPointArchiveFactory dbFactory, DatabaseMetrics dbMetrics,
                       DataPointStagingStore stagingStore, boolean updateLowerResolutionArchives,
                       int timeSeriesCacheMaxSize, int timeSeriesCacheExpireInSec,
                       QueryCachePolicy queryCachePolicy, Predicate<String> metricNamePresentPredicate)
    {
        this.dbFactory = Preconditions.checkNotNull( dbFactory );
        this.dbMetrics = Preconditions.checkNotNull( dbMetrics );
        this.stagingStore = Preconditions.checkNotNull( stagingStore );
        this.updateLowerResolutionArchives = updateLowerResolutionArchives;
        this.queryCachePolicy = Preconditions.checkNotNull( queryCachePolicy );
        this.metricNamePresent = metricNamePresentPredicate;
        seriesCache =
            CacheBuilder.newBuilder().initialCapacity( timeSeriesCacheMaxSize ).maximumSize( timeSeriesCacheMaxSize )
                .recordStats().concurrencyLevel( 8 ).expireAfterWrite( timeSeriesCacheExpireInSec, TimeUnit.SECONDS )
                .build( new CacheLoader<SeriesCacheKey, Series>()
                {
                    @Override
                    public Series load( SeriesCacheKey key )
                        throws Exception
                    {
                        return getSeries( key );
                    }
                } );
        seriesCacheStatsReporter = new CacheStatsReporter( metricRegistry,"SeriesQueryResults", timeSeriesCacheMaxSize, seriesCache );
    }

//    @Override
//    public void setMetricNamePresentPredicate(Predicate<String> p)
//    {
//        this.isMetricNamePresent = p;
//    }


    @Override
    public void dumpStats()
    {
        dbFactory.dumpStats();
        stagingStore.dumpStats();
        seriesCacheStatsReporter.dumpStats();
    }

    @Override
    public void open()
    {
        stagingStore.open( this );
    }

    @Override
    public List<DataPointValue> getValues( RetentionPolicy archivePolicy, long metricId, int from, int to )
    {
        DataPointArchive db = dbFactory.get( archivePolicy );
        return db.getDataPoints( metricId, from, to );
    }

    @Override
    public DataPointImportResults importDataPoints( String dbName, List<DataPoint> points, int maxAllowedImportErrors )
    {
        RetentionPolicy rp = RetentionPolicy.getInstanceForDbName( dbName );
        DataPointArchive db = dbFactory.get( rp );

        int errCount = 0;
        int expiredCount = 0;
        int savedCount = 0;

        for ( DataPoint p : points )
        {
            try
            {
                importDataPoint( rp, db, p );
                savedCount++;
            }
            catch ( ExpiredOnArrivalDataPointException e )
            {
                expiredCount++;
            }
            catch ( RuntimeException e )
            {
                errCount++;
                if ( log.isDebugEnabled() )
                {
                    log.debug( "Failed to import data point [" + p + "]", e );
                }
                if ( errCount > maxAllowedImportErrors )
                {
                    throw new RuntimeException( String.format(
                        "Number of errors [%s] exceeds specified maxAllowedImportErrors [%s] for the request",
                        errCount, maxAllowedImportErrors ) );
                }
            }
        }
        return new DataPointImportResults( dbName, points.size(), savedCount, errCount, expiredCount );
    }

    private void importDataPoint( RetentionPolicy rp, DataPointArchive db, DataPoint p )
    {
        if ( !p.hasMetricId() )
        {
            throw new RuntimeException( "Failed to import data point - metric id is required. Point: [" + p + "]" );
        }

        rp.assertTimestampMatchesThisPolicyInterval( p.ts );

        int now = timeSource.getEpochSecond();
        if ( rp.includes( p.ts, now ) )
        {
            db.put( p.metricId, p.ts, p.val );
        }
        else
        {
            throw new ExpiredOnArrivalDataPointException( p.ts, p.val, rp );
        }
    }

    @Override
    public void delete( List<Metric> metrics )
    {
        for ( Metric m : metrics )
        {
            if ( m.isLeaf() )
            {
                for ( RetentionPolicy rp : m.getRetentionPolicies() )
                {
                    DataPointArchive db = dbFactory.get( rp );
                    db.deleteMetric( m.id );
                }
            }
        }
    }

    @Override
    public long delete( String dbName, int ts )
    {
        RetentionPolicy rp = RetentionPolicy.getInstanceForDbName( dbName );
        DataPointArchive db = dbFactory.get( rp );
        return db.delete( rp.interval( ts ) );
    }

    @Override
    public Series getSeries( Metric metric, int from, int until, int now )
    {
        dbMetrics.markQueriesServed();
        try
        {
            RetentionPolicy dbPolicy = metric.pickArchiveForQuery( from, until, now ).orElse( null );
            SeriesCacheKey key = new SeriesCacheKey( metric, from, until, now );
            if( queryCachePolicy.useCache( dbPolicy ) )
            {
                return seriesCache.get( key );
            }
            else
            {
                return getSeries( key );
            }
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Series getSeries( SeriesCacheKey key )
    {
        RetentionPolicy archivePolicy = key.m.pickArchiveForQuery( key.from, key.until, key.now ).orElse( null );
        if ( archivePolicy != null )
        {
            int from = archivePolicy.interval( key.from );
            int step = archivePolicy.precision;
            int until = archivePolicy.interval( key.until );
            List<Double> points;

            // for empty one
            if( (archivePolicy.is5m7d() || archivePolicy.is60s24h()) && !metricNamePresent.test(key.m.name)  )
            {
                DatabaseMetrics.obsoleteSeriesAccessMeter.mark();

                points = new ArrayList<>();
                int ts = from;
                while ( ts <= until )
                {
                    points.add( null );
                    ts = ts + step;
                }
            }
            else
            {
                DataPointArchive db = dbFactory.get(archivePolicy);
                points = db.getDataPoints(key.m.id, from, until, step);
            }
            return new Series( key.m.name, from, until, step, points );
        }
        else
        {
            // TODO: check what python code is doing in such cases?
            // TODO: maybe just use lowest precision archive and return list of null values?
            return new Series( key.m.name, key.from, key.until, key.now, new ArrayList<Double>() );
        }
    }


    @Override
    public void close()
    {
        stagingStore.closeQuietly();
        dbFactory.close();
        seriesCacheStatsReporter.close();
    }

    @Override
    public void insertDataPoints( DataPoints points )
    {
        Set<RetentionPolicy> pointPolicies = points.getPresentPolicies();
        if ( pointPolicies == null || pointPolicies.size() == 0 )
        {
            log.debug( String.format( "there were no point retention policies provided. data point batch size: %s",
                points.size() ) );
            return;
        }

        for ( RetentionPolicy policy : pointPolicies )
        {
            DataPointArchive db = dbFactory.get( policy );
            int n = db.put( points );
            dbMetrics.markPointsSaved( n );
        }

        if ( updateLowerResolutionArchives )
        {
            Metric m;
            DataPoint p;
            for ( int i = 0, n = points.size(); i < n; i++ )
            {
                p = points.get( i );
                if ( !p.isValid() )
                {
                    continue;
                }

                if ( !p.hasMetricId() )
                {
                    // skip new data point until metric name is created
                    continue;
                }

                m = points.getMetric( i );
                RetentionPolicy policy = points.getPolicy( i );
                if ( m == null || policy == null )
                {
                    log.error(String.format(
                        "Invalid state for low res point. Metric [%s], Retention policy [%s]. Point [%s],  Point metric id [%s]",
                            m, policy, p, p.metricId ));
                    continue;
                }
                // send to staging...
                RetentionPolicy nextPolicy = m.retentionPolicyAfter( policy ).orElse( null );
                if ( nextPolicy != null )
                {
                    updateLowerResolutionArchive( m, p.ts, p.val, nextPolicy );
                }
            }
        }
    }

    private void updateLowerResolutionArchive( Metric metric, int ts, double val, RetentionPolicy nextPolicy )
    {
        DataPointArchive nextArchive = dbFactory.get( nextPolicy );
        String dbName = nextArchive.getName();
        // from also identifies interval in nextArchive for this data point
        int from = nextPolicy.interval( ts );
        stagingStore.add( dbName, from, metric.id, val );
    }

    @Override
    public void importDataPoints( String dbName, DataPoints points )
    {
        DataPointArchive db = dbFactory.get( dbName );
        db.put( points );
    }

    @Override
    public DataPointValue getFirst( RetentionPolicy archivePolicy, long metricId, int from, int to )
    {
        DataPointArchive db = dbFactory.get( archivePolicy );
        return db.getFirst( metricId, from, to );
    }
}
