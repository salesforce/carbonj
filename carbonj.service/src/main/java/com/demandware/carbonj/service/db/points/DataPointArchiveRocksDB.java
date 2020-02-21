/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import static com.demandware.carbonj.service.db.util.MetricFormatUtils.formatDBReadWriteTimerStats;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.*;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

class DataPointArchiveRocksDB
    implements DataPointArchive
{
    private static Logger log = LoggerFactory.getLogger( DataPointArchiveRocksDB.class );

    final private File dbDir;

    final private String dbName;

    final private RetentionPolicy policy;

    private TtlDB db;

    private Timer writeTimer;

    private Timer batchWriteTimer;

    private Meter savedRecordsMeter;

    private Timer readTimer;

    private Timer deleteTimer;

    private ReadOptions readOptions;

    private WriteOptions writeOptions;

    private RocksDBConfig rocksdbConfig;

    private Timer emptyReadTimer;

    private TimeSource timeSource = TimeSource.defaultTimeSource();

    private ThreadPoolExecutor cleaner = new ThreadPoolExecutor( 1, 1, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(
        100000 ), new ThreadFactoryBuilder().setDaemon( true ).build(), new ThreadPoolExecutor.DiscardPolicy()
    {
        @Override
        public void rejectedExecution( Runnable r, ThreadPoolExecutor e )
        {
            log.info( "cleaner queue is full. rejecting (GC should pick this object up eventually)" );
            super.rejectedExecution( r, e );
        }
    } );

    DataPointArchiveRocksDB(MetricRegistry metricRegistry, String dbName, RetentionPolicy policy, File dbDir, RocksDBConfig rocksdbConfig )
    {
        this.dbName = Preconditions.checkNotNull( dbName );
        this.policy = Preconditions.checkNotNull( policy );
        this.dbDir = Preconditions.checkNotNull( dbDir );
        this.rocksdbConfig = Preconditions.checkNotNull( rocksdbConfig );
        this.savedRecordsMeter = metricRegistry.meter( dbSavedRecordsMeterName( dbName ) );
        this.writeTimer = metricRegistry.timer( dbWriteTimerName( dbName ) );
        this.batchWriteTimer = metricRegistry.timer( dbBatchWriteTimerName( dbName ) );
        this.readTimer = metricRegistry.timer( dbReadTimerName( dbName ) );
        this.emptyReadTimer = metricRegistry.timer( dbEmptyReadTimerName( dbName ) );
        this.deleteTimer = metricRegistry.timer( dbDeleteTimerName( dbName ) );
        TtlDB.loadLibrary();

    }

    @Override
    public void dumpStats()
    {
        String readStats = String.format( "READ(%s)", formatDBReadWriteTimerStats( readTimer ) );
        String writeStats = String.format( "WRITE(%s)", formatDBReadWriteTimerStats( writeTimer ) );
        log.info( String.format( "Data points store %s. %s %s", getName(), readStats, writeStats ) );
        try
        {
            log.info( String.format( "RocksDB Memory usage: index and filter blocks [%s]",
                this.db.getProperty( "rocksdb.estimate-table-readers-mem" ) ) );
        }
        catch ( Throwable t )
        {
            Throwables.propagate( t );
        }

    }

    @Override
    public String getName()
    {
        return dbName;
    }

    @Override
    public void deleteMetric( int metricId )
    {
        deleteMetric( metricId, 0, Integer.MAX_VALUE );
    }

    @Override
    public void deleteMetric( int metricId, int from, int until )
    {
        RocksIterator iter = null;
        try
        {
            iter = db.newIterator( readOptions );
            byte[] startKey = DataPointRecord.toKeyBytes( metricId, 0 );
            byte[] endKey = DataPointRecord.toKeyBytes( metricId, Integer.MAX_VALUE );

            for ( iter.seek( startKey ); iter.isValid(); iter.next() )
            {
                byte[] key = iter.key();
                if ( keyCompare( key, endKey ) > 0 )
                {
                    break;
                }

                dbDelete( key );
            }
        }
        finally
        {
            if ( iter != null )
            {
                final RocksIterator iterToDispose = iter;
                // contains global lock. Dispose in a separate thread to avoid contention.
                cleaner.execute( ( ) -> dispose( iterToDispose ) );
            }
        }
    }

    @Override
    public long delete( int ts )
    {
        final Timer.Context timerContext = deleteTimer.time();
        long c = 0;
        long i = 0;
        RocksIterator iter = null;
        try
        {
            iter = db.newIterator( readOptions );
            for ( iter.seekToFirst(); iter.isValid(); iter.next(), i++ )
            {
                byte[] key = iter.key();
                if ( DataPointRecord.toTimestamp( key ) == ts )
                {
                    try
                    {
                        db.remove( key );
                    }
                    catch ( RocksDBException e )
                    {
                        Throwables.propagate( e );
                    }
                    c++;
                }
                if ( i % 1000000 == 0 )
                {
                    log.info( String.format( "delete action checked %s records and removed %s", i, c ) );
                }
            }
        }
        finally
        {
            long elapsedNanos = timerContext.stop();
            log.info( String.format( "Spent %s ms to delete %s points from %s in %s",
                TimeUnit.NANOSECONDS.toMillis( elapsedNanos ), c, i, dbName ) );

            if ( iter != null )
            {
                final RocksIterator iterToDispose = iter;
                // contains global lock. Dispose in a separate thread to avoid contention.
                cleaner.execute( ( ) -> dispose( iterToDispose ) );
            }
        }

        return c;
    }


    private void dbDelete( byte[] key )
    {
        try
        {
            db.remove( key );
        }
        catch ( RocksDBException e )
        {
            String msg = "Failed to remove record. db [" + dbName + "], key [" + Arrays.toString( key ) + "]";

            try
            {
                int mId = DataPointRecord.toMetricId( key );
                int ts = DataPointRecord.toTimestamp( key );
                msg = "Failed to remove record. db [" + dbName + "], key [" + mId + ":" + ts + "]";
            }
            catch ( Throwable t )
            {
                log.warn( "Failed to deserialize the key: " + Arrays.toString( key ) );
            }
            throw new RuntimeException( msg );
        }
    }
    @Override
    public int put( DataPoints points )
    {
        WriteBatch batch = new WriteBatch();
        int now = timeSource.getEpochSecond();
        try {
            for (int i = 0, n = points.size(); i < n; i++) {
                DataPoint p = points.get(i);
                if (!p.isValid()) {
                    continue;
                }

                if (!p.hasMetricId()) {
                    continue;
                }

                if (!isWithinRetentionPeriod(p, now)) {
                    continue;
                }

                Metric m = points.getMetric(i);
                RetentionPolicy pointPolicy = points.getPolicy(i);
                if (m == null || pointPolicy == null) {
                    log.error(String
                            .format(
                                    "Skip point for invalid metric. Metric [%s] with child nodes [%s] doesn't have retention policy. Point [%s],  Point metric id [%s]",
                                    m, null == m ? null : m.children(), p, p.metricId));
                    continue;
                }

                // exclude points that have policy for different db
                if (dbName.equals(pointPolicy.dbName)) {
                    int interval = policy.interval(p.ts);
                    byte[] key = DataPointRecord.toKeyBytes(p.metricId, interval);
                    byte[] value = DataPointRecord.toValueBytes(p.val);
                    batch.put(key, value);
                }
            }
            int batchSize = batch.count();
            final Timer.Context timerContext = batchWriteTimer.time();
            db.write(writeOptions, batch);
            savedRecordsMeter.mark( batchSize );
            timerContext.stop();
            return batchSize;
        }
        catch ( RocksDBException e )
        {
            Throwables.propagate( e );
        }
        finally
        {
            dispose( batch );
        }
        return 0;  // not reached in case of exception
    }

    private boolean isWithinRetentionPeriod(DataPoint p, int now) {
        return policy.includes(p.ts, now);
    }

    @Override
    public void put( int metricId, int interval, double v )
    {
        byte[] key = DataPointRecord.toKeyBytes( metricId, interval );
        byte[] value = DataPointRecord.toValueBytes( v );
        final Timer.Context timerContext = writeTimer.time();
        try
        {
            db.put( writeOptions, key, value );
        }
        catch ( RocksDBException e )
        {
            Throwables.propagate( e );
        }
        finally
        {
            timerContext.stop();
        }
    }

    @Override
    public List<DataPointValue> getDataPoints( int metricId, int startTime, int endTime )
    {
        return getDataPointsWithLimit( metricId, startTime, endTime, Integer.MAX_VALUE );
    }

    private List<DataPointValue> getDataPointsWithLimit( int metricId, int startTime, int endTime, int resultLimit )
    {
        List<DataPointValue> points = new ArrayList<>();
        RocksIterator iter = null;
        try
        {
            iter = db.newIterator( readOptions );
            byte[] startKey = DataPointRecord.toKeyBytes( metricId, startTime );
            byte[] endKey = DataPointRecord.toKeyBytes( metricId, endTime );

            for ( iter.seek( startKey ); iter.isValid() && points.size() < resultLimit; iter.next() )
            {
                byte[] key = iter.key();
                if ( keyCompare( key, endKey ) > 0 )
                {
                    break;
                }

                byte[] value = iter.value();

                int ts = DataPointRecord.toTimestamp( key );
                double val = DataPointRecord.toValue( value );
                DataPointValue dpv = new DataPointValue( ts, val );
                points.add( dpv );
            }
        }
        finally
        {
            dispose( iter );
        }

        return points;

    }

    private void dispose( RocksObject o )
    {
        if ( o != null )
        {
            // contains global lock. Dispose in a separate thread to avoid contention.
            cleaner.execute( ( ) -> o.dispose() );
        }
    }

    @Override
    public List<Double> getDataPoints( int metricId, int startTime, int endTime, int step )
    {
        // TODO: SB code duplication!!! the method needs to be rewritten to utilize getDataPoints()
        // API and implementation needs to be consistent.
        boolean emptyRead = true;
        final Timer.Context timerContext = readTimer.time();
        // TODO: use array instead
        List<Double> points = new ArrayList<>();
        RocksIterator iter = null;
        try
        {
            // TODO: just to get started. Revisit as part of tuning.
            iter = db.newIterator( readOptions );
            byte[] startKey = DataPointRecord.toKeyBytes( metricId, startTime );
            byte[] endKey = DataPointRecord.toKeyBytes( metricId, endTime );

            int ts = startTime;

            iter.seek( startKey );

            for ( ; iter.isValid(); iter.next() )
            {
                byte[] key = iter.key();
                if ( keyCompare( key, endKey ) > 0 )
                {
                    break;
                }

                byte[] value = iter.value();

                int iterTime = DataPointRecord.toTimestamp( key );

                // fill missing intervals with null
                while ( ts < iterTime )
                {
                    points.add( null );
                    ts = ts + step;
                }
                emptyRead = false;
                points.add( DataPointRecord.toValue( value ) );
                ts = ts + step; // next expected interval
            }

            while ( ts <= endTime )
            {
                points.add( null );
                ts = ts + step;
            }
        }
        finally
        {
            long d = timerContext.stop();
            if( emptyRead )
            {
                emptyReadTimer.update(d, TimeUnit.NANOSECONDS);
            }

            if ( iter != null )
            {
                final RocksIterator iterToDispose = iter;
                // contains global lock. Dispose in a separate thread to avoid contention.
                cleaner.execute( ( ) -> dispose( iterToDispose ) );
            }
        }

        return points;
    }

    private static int keyCompare( byte[] keyBytes1, byte[] keyBytes2 )
    {
        return UnsignedBytes.lexicographicalComparator().compare( keyBytes1, keyBytes2 );
    }

    @Override
    public void open()
    {
        log.info( "Opening rocksdb '" + dbName + "'. Config options: " + rocksdbConfig );
        // TODO: these are just baseline options to get started. Revisit as part of tuning.

        Env env = Env.getDefault();
        env.setBackgroundThreads( rocksdbConfig.compactionThreadPoolSize, Priority.LOW );
        env.setBackgroundThreads( rocksdbConfig.flushThreadPoolSize, Priority.HIGH );

        Options options =
            new Options()
                .setCreateIfMissing( true )
                .setMaxOpenFiles( -1 )
                // also check OS config will support this setting
                .setIncreaseParallelism( rocksdbConfig.increaseParallelism )
                .setMaxBackgroundCompactions( rocksdbConfig.maxBackgroundCompactions )
                .setMaxBackgroundFlushes( rocksdbConfig.maxBackgroundFlushes )
                // .setTableFormatConfig( cfg )
                // .setArenaBlockSize( )
                .setMaxWriteBufferNumber( rocksdbConfig.maxWriteBufferNumber )
                .setWriteBufferSize( rocksdbConfig.writeBufferSize )
                .setTargetFileSizeBase( rocksdbConfig.targetFileSizeBase )
                .setTargetFileSizeMultiplier( rocksdbConfig.targetFileSizeMultiplier )
                .setNumLevels( rocksdbConfig.numLevels ).setMaxBytesForLevelBase( rocksdbConfig.maxBytesForLevelBase )
                .setMaxBytesForLevelMultiplier( rocksdbConfig.maxBytesForLevelMultiplier )
                .setLevelZeroFileNumCompactionTrigger( rocksdbConfig.levelZeroFileNumCompactionTrigger )
                .setLevelZeroSlowdownWritesTrigger( rocksdbConfig.levelZeroSlowDownWritesTrigger )
                .setLevelZeroStopWritesTrigger( rocksdbConfig.levelZeroStopWritesTrigger ).setEnv( env )
                .setMinWriteBufferNumberToMerge( rocksdbConfig.minWriteBufferNumberToMerge )
                .optimizeLevelStyleCompaction()
                .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
                .setLevelCompactionDynamicLevelBytes(true)
                .setCompressionType( CompressionType.ZSTD_COMPRESSION );

        if ( rocksdbConfig.bytesPerSync > 0 )
        {
            options.setBytesPerSync( 0 );
        }

        if ( rocksdbConfig.useBlockBasedTableConfig )
        {
            BlockBasedTableConfig cfg = new BlockBasedTableConfig();
            if ( rocksdbConfig.useBloomFilter )
            {
                BloomFilter filter = new BloomFilter( 10 );
                cfg.setFilterPolicy( filter );
            }
            cfg.setBlockCache( new LRUCache(rocksdbConfig.blockCacheSize) );
            cfg.setBlockSize( rocksdbConfig.blockSize );
            cfg.setCacheIndexAndFilterBlocks(true);
            cfg.setPinL0FilterAndIndexBlocksInCache(true);

            options.setTableFormatConfig( cfg );
        }

        int ttl = policy.retention;
        try
        {
            db = TtlDB.open( options, dbDir.getAbsolutePath(), ttl, false );
        }
        catch ( RocksDBException e )
        {
            throw Throwables.propagate( e );
        }

        readOptions = new ReadOptions();

        writeOptions = new WriteOptions();
        writeOptions.setDisableWAL( rocksdbConfig.disableWAL );
    }

    private void closeQuietly( RocksDB db )
    {

        if ( db != null )
        {
            try
            {
                db.close();
            }
            catch ( Exception e )
            {
                log.error( "Error while closing data point archive database [" + dbName + "].", e );
            }
        }
    }

    @Override
    public void close()
    {
        log.info( "closing data point archive database [" + dbName + "]" );
        closeQuietly( db );
        log.info( "closed data point archive database [" + dbName + "]." );
    }

    private String dbWriteTimerName( String dbName )
    {
        return "db." + dbName + ".write.time";
    }

    private String dbBatchWriteTimerName( String dbName )
    {
        return "db." + dbName + ".batchWrite.time";
    }

    private String dbSavedRecordsMeterName( String dbName )
    {
        return "db." + dbName + ".records.saved";
    }

    private String dbReadTimerName( String dbName )
    {
        return "db." + dbName + ".read.time";
    }

    private String dbEmptyReadTimerName( String dbName )
    {
        return "db." + dbName + ".emptyRead.time";
    }

    private String dbDeleteTimerName( String dbName )
    {
        return "db." + dbName + ".delete.time";
    }

    @Override
    public DataPointValue getFirst( int metricId, int from, int to )
    {
        List<DataPointValue> r = getDataPointsWithLimit( metricId, from, to, 1 );
        return r.isEmpty() ? null : r.get( 0 );
    }

}
