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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.SyncPrimaryDbTask;
import com.demandware.carbonj.service.db.index.NameUtils;
import com.demandware.carbonj.service.db.util.MetricUtils;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.Priority;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger( DataPointArchiveRocksDB.class );

    final private File dbDir;

    final private String dbName;

    final private RetentionPolicy policy;

    private RocksDB db;

    private final Timer writeTimer;

    private final Timer batchWriteTimer;

    private final Meter savedRecordsMeter;

    private final Timer readTimer;

    private final Timer deleteTimer;

    private ReadOptions readOptions;

    private WriteOptions writeOptions;

    private final RocksDBConfig rocksdbConfig;

    private final Timer emptyReadTimer;

    private final TimeSource timeSource = TimeSource.defaultTimeSource();

    private final boolean longId;

    private final File secondaryDbDir;

    private final Timer catchUpTimer;

    private final Meter catchUpTimerError;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final ThreadPoolExecutor cleaner;

    private final ConcurrentMap<String, Histogram> latencyByNamespaceMap = new ConcurrentHashMap<>();

    private final MetricRegistry metricRegistry;

    private final NameUtils nameUtils = new NameUtils();

    DataPointArchiveRocksDB(MetricRegistry metricRegistry,
                                String dbName,
                                RetentionPolicy policy,
                                File dbDir,
                                RocksDBConfig rocksdbConfig,
                                boolean longId)
    {
        this.metricRegistry = metricRegistry;
        this.dbName = Preconditions.checkNotNull( dbName );
        this.policy = Preconditions.checkNotNull( policy );
        this.dbDir = Preconditions.checkNotNull( dbDir );
        this.secondaryDbDir = new File(dbDir.getParentFile(), dbName + "-secondary");
        this.rocksdbConfig = Preconditions.checkNotNull( rocksdbConfig );
        this.savedRecordsMeter = metricRegistry.meter(MetricUtils.dbSavedRecordsMeterName(dbName));
        this.writeTimer = metricRegistry.timer(MetricUtils.dbWriteTimerName(dbName));
        this.batchWriteTimer = metricRegistry.timer(MetricUtils.dbBatchWriteTimerName(dbName));
        this.readTimer = metricRegistry.timer(MetricUtils.dbReadTimerName(dbName));
        this.emptyReadTimer = metricRegistry.timer(MetricUtils. dbEmptyReadTimerName(dbName));
        this.deleteTimer = metricRegistry.timer(MetricUtils.dbDeleteTimerName(dbName));
        this.catchUpTimer = metricRegistry.timer(MetricUtils.dbCatchUpTimerName(dbName));
        this.catchUpTimerError = metricRegistry.meter(MetricUtils.dbCatchUpTimerErrorName(dbName));
        this.longId = longId;
        this.cleaner = new ThreadPoolExecutor( 1, 1, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(
                100000 ), new ThreadFactoryBuilder().setDaemon( true ).build(), new ThreadPoolExecutor.DiscardPolicy()
        {
            @Override
            public void rejectedExecution( Runnable r, ThreadPoolExecutor e )
            {
                log.info( "cleaner queue is full. rejecting (GC should pick this object up eventually)" );
                super.rejectedExecution( r, e );
            }
        } );
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
            Throwables.throwIfUnchecked(t);
        }

    }

    @Override
    public String getName()
    {
        return dbName;
    }

    @Override
    public void deleteMetric( long metricId )
    {
        deleteMetric( metricId, 0, Integer.MAX_VALUE );
    }

    @Override
    public void deleteMetric( long metricId, int from, int until )
    {
        if (rocksdbConfig.readOnly) {
            throw new UnsupportedOperationException("Method deleteMetric is not supported for readonly mode");
        }
        RocksIterator iter = null;
        try
        {
            iter = db.newIterator( readOptions );
            byte[] startKey = DataPointRecord.toKeyBytes( metricId, 0 , longId);
            byte[] endKey = DataPointRecord.toKeyBytes( metricId, Integer.MAX_VALUE, longId );

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
        if (rocksdbConfig.readOnly) {
            throw new UnsupportedOperationException("Method delete is not supported for readonly mode");
        }
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
                if ( DataPointRecord.toTimestamp( key, longId ) == ts )
                {
                    try
                    {
                        db.delete( key );
                    }
                    catch ( RocksDBException e )
                    {
                        Throwables.throwIfUnchecked(e);
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
                cleaner.execute(() -> dispose(iterToDispose));
            }
        }

        return c;
    }


    private void dbDelete( byte[] key )
    {
        if (rocksdbConfig.readOnly) {
            throw new UnsupportedOperationException("Method dbDelete is not supported for readonly mode");
        }
        try
        {
            db.delete( key );
        }
        catch ( RocksDBException e )
        {
            String msg = "Failed to remove record. db [" + dbName + "], key [" + Arrays.toString( key ) + "]";

            try
            {
                long mId = DataPointRecord.toMetricId( key, longId );
                int ts = DataPointRecord.toTimestamp( key, longId );
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
        if (rocksdbConfig.readOnly) {
            throw new UnsupportedOperationException("Method put is not supported for readonly mode");
        }
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
                    byte[] key = DataPointRecord.toKeyBytes(p.metricId, interval, longId);
                    byte[] value = DataPointRecord.toValueBytes(p.val);
                    batch.put(key, value);
                    String namespace = nameUtils.firstSegment(p.name);
                    if (!latencyByNamespaceMap.containsKey(namespace)) {
                        Histogram latency = metricRegistry.histogram(MetricRegistry.name(MetricUtils.dbDataPointLatencyName(dbName, namespace)));
                        latencyByNamespaceMap.putIfAbsent(namespace, latency);
                    }
                    latencyByNamespaceMap.get(namespace).update(now - p.ts);
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
            Throwables.throwIfUnchecked(e);
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
    public void put( long metricId, int interval, double v )
    {
        if (rocksdbConfig.readOnly) {
            throw new UnsupportedOperationException("Method put is not supported for readonly mode");
        }
        byte[] key = DataPointRecord.toKeyBytes( metricId, interval, longId );
        byte[] value = DataPointRecord.toValueBytes( v );
        try (Timer.Context ignored = writeTimer.time())
        {
            db.put( writeOptions, key, value );
        }
        catch ( RocksDBException e )
        {
            Throwables.throwIfUnchecked(e);
        }
    }

    @Override
    public List<DataPointValue> getDataPoints( long metricId, int startTime, int endTime )
    {
        return getDataPointsWithLimit( metricId, startTime, endTime, Integer.MAX_VALUE );
    }

    private List<DataPointValue> getDataPointsWithLimit( long metricId, int startTime, int endTime, int resultLimit )
    {
        List<DataPointValue> points = new ArrayList<>();
        RocksIterator iter = null;
        try
        {
            iter = db.newIterator( readOptions );
            byte[] startKey = DataPointRecord.toKeyBytes( metricId, startTime, longId );
            byte[] endKey = DataPointRecord.toKeyBytes( metricId, endTime, longId );

            for ( iter.seek( startKey ); iter.isValid() && points.size() < resultLimit; iter.next() )
            {
                byte[] key = iter.key();
                if ( keyCompare( key, endKey ) > 0 )
                {
                    break;
                }

                byte[] value = iter.value();

                int ts = DataPointRecord.toTimestamp( key, longId );
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
            cleaner.execute(o::close);
        }
    }

    @Override
    public List<Double> getDataPoints( long metricId, int startTime, int endTime, int step )
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
            byte[] startKey = DataPointRecord.toKeyBytes( metricId, startTime, longId );
            byte[] endKey = DataPointRecord.toKeyBytes( metricId, endTime, longId );

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

                int iterTime = DataPointRecord.toTimestamp( key, longId );

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
                cleaner.execute(() -> dispose(iterToDispose));
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

        Options options = new Options().setMaxOpenFiles( -1 );
        if (rocksdbConfig.readOnly) {
            options.setCreateIfMissing(false);
        } else {
            // TODO: these are just baseline options to get started. Revisit as part of tuning.
            Env env = Env.getDefault();
            env.setBackgroundThreads( rocksdbConfig.compactionThreadPoolSize, Priority.LOW );
            env.setBackgroundThreads( rocksdbConfig.flushThreadPoolSize, Priority.HIGH );

            options
                    .setCreateIfMissing(true)
                    // also check OS config will support this setting
                    .setIncreaseParallelism(rocksdbConfig.increaseParallelism)
                    .setMaxBackgroundCompactions(rocksdbConfig.maxBackgroundCompactions)
                    .setMaxBackgroundFlushes(rocksdbConfig.maxBackgroundFlushes)
                    // .setTableFormatConfig( cfg )
                    // .setArenaBlockSize( )
                    .setMaxWriteBufferNumber(rocksdbConfig.maxWriteBufferNumber)
                    .setWriteBufferSize(rocksdbConfig.writeBufferSize)
                    .setTargetFileSizeBase(rocksdbConfig.targetFileSizeBase)
                    .setTargetFileSizeMultiplier(rocksdbConfig.targetFileSizeMultiplier)
                    .setNumLevels(rocksdbConfig.numLevels).setMaxBytesForLevelBase(rocksdbConfig.maxBytesForLevelBase)
                    .setMaxBytesForLevelMultiplier(rocksdbConfig.maxBytesForLevelMultiplier)
                    .setLevelZeroFileNumCompactionTrigger(rocksdbConfig.levelZeroFileNumCompactionTrigger)
                    .setLevelZeroSlowdownWritesTrigger(rocksdbConfig.levelZeroSlowDownWritesTrigger)
                    .setLevelZeroStopWritesTrigger(rocksdbConfig.levelZeroStopWritesTrigger).setEnv(env)
                    .setMinWriteBufferNumberToMerge(rocksdbConfig.minWriteBufferNumberToMerge)
                    .optimizeLevelStyleCompaction()
                    .setCompactionPriority(CompactionPriority.MinOverlappingRatio)
                    .setLevelCompactionDynamicLevelBytes(true)
                    .setCompressionType(CompressionType.ZSTD_COMPRESSION);

            if (rocksdbConfig.bytesPerSync > 0) {
                options.setBytesPerSync(0);
            }

            if (rocksdbConfig.useBlockBasedTableConfig) {
                BlockBasedTableConfig cfg = new BlockBasedTableConfig();
                if (rocksdbConfig.useBloomFilter) {
                    BloomFilter filter = new BloomFilter(10);
                    cfg.setFilterPolicy(filter);
                }
                cfg.setBlockCache(new LRUCache(rocksdbConfig.blockCacheSize));
                cfg.setBlockSize(rocksdbConfig.blockSize);
                cfg.setCacheIndexAndFilterBlocks(true);
                cfg.setPinL0FilterAndIndexBlocksInCache(true);

                options.setTableFormatConfig(cfg);
            }
        }

        readOptions = new ReadOptions();
        writeOptions = new WriteOptions();
        int ttl = policy.retention;
        try
        {
            if (rocksdbConfig.readOnly) {
                db = RocksDB.openAsSecondary(options, dbDir.getAbsolutePath(), secondaryDbDir.getAbsolutePath());
                log.info("Rocks DB {} opened in secondary mode", dbName);
                scheduledExecutorService.scheduleAtFixedRate(
                        new SyncPrimaryDbTask(db, dbDir, catchUpTimer, catchUpTimerError, rocksdbConfig.catchupRetry),
                        60, 60, TimeUnit.SECONDS);
            } else {
                db = TtlDB.open(options, dbDir.getAbsolutePath(), ttl, false);
                writeOptions.setDisableWAL( rocksdbConfig.disableWAL );
                log.info("Rocks DB {} opened in normal mode", dbName);
            }
        }
        catch ( RocksDBException e )
        {
            Throwables.throwIfUnchecked(e);
        }
    }

    private void closeQuietly( RocksDB db )
    {
        if (rocksdbConfig.readOnly) {
            scheduledExecutorService.shutdownNow();
        }

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

    @Override
    public DataPointValue getFirst( long metricId, int from, int to )
    {
        List<DataPointValue> r = getDataPointsWithLimit( metricId, from, to, 1 );
        return r.isEmpty() ? null : r.get( 0 );
    }
}
