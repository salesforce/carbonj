/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.primitives.SignedBytes;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class IndexStoreRocksDB<K, R extends Record<K>>
    implements IndexStore<K, R>
{
    private static final Logger log = LoggerFactory.getLogger( IndexStoreRocksDB.class );

    final private String dbName;

    final private File dbDir;

    private final File secondaryDbDir;

    private RocksDB db;

    final private RecordSerializer<K, R> recSerializer;

    private final Timer writeTimer;

    private final Timer readTimer;

    private final Timer delTimer;

    private final boolean rocksdbReadonly;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public IndexStoreRocksDB(MetricRegistry metricRegistry, String dbName, File dbDir, RecordSerializer<K, R> recSerializer, boolean rocksdbReadonly)
    {
        this.dbName = Preconditions.checkNotNull( dbName );
        this.dbDir = Preconditions.checkNotNull( dbDir );
        this.secondaryDbDir = new File(dbDir.getParentFile(), dbName + "-secondary");
        this.recSerializer = recSerializer;
        this.writeTimer = metricRegistry.timer( dbWriteTimerName( dbName ) );
        this.readTimer = metricRegistry.timer( dbReadTimerName( dbName ) );
        this.delTimer = metricRegistry.timer( dbDeleteTimerName( dbName ) );
        this.rocksdbReadonly = rocksdbReadonly;
    }

    @Override
    public void dumpStats()
    {
    }

    @Override
    public void open()
    {
        log.info("Opening RocksDB metric index store in [{}]", dbDir);

        TtlDB.loadLibrary();

        Options options = new Options().setCompressionType( CompressionType.SNAPPY_COMPRESSION );

        try
        {
            if (rocksdbReadonly) {
                options.setCreateIfMissing(false);
                options.setMaxOpenFiles(-1);
                this.db = RocksDB.openAsSecondary(options, dbDir.getAbsolutePath(), secondaryDbDir.getAbsolutePath());
                log.info("RocksDB metric index store in [{}] opened in secondary mode", dbDir);
                scheduledExecutorService.scheduleAtFixedRate(new SyncPrimaryDbTask(db, dbDir), 60, 60, TimeUnit.SECONDS);
            } else {
                options.setCreateIfMissing(true);
                this.db = RocksDB.open(options, dbDir.getAbsolutePath());
                log.info("RocksDB metric index store in [{}] opened in normal mode", dbDir);
            }
        }
        catch ( RocksDBException e )
        {
            //noinspection deprecation
            throw Throwables.propagate( e );
        }
    }

    @Override
    public void dump( PrintWriter pw )
    {
        scan( null, null, pw::println);
    }

    private static int keyCompare( byte[] keyBytes1, byte[] keyBytes2 )
    {
        // Since few of the old shards are in negative, ids are no more unsigned
        return SignedBytes.lexicographicalComparator().compare( keyBytes1, keyBytes2 );
    }

    @Override
    public long scan( K startKey, K endKey, Consumer<R> c )
    {
        long processed = 0;
        byte[] endKeyBytes = null == endKey ? null : recSerializer.keyBytes( endKey );
        try (RocksIterator iter = db.newIterator( new ReadOptions() ))
        {
            // Rocksdb jni does not support min negative value - Integer_MAX_VALUE + 1.
            // This is a work around to seek to the first value.
            // Would like to change the signature of the method to concrete type - long but
            // that needs lot of changes.
            if ( null == startKey  || (long)startKey < 0)
            {
                iter.seekToFirst();
            }
            else
            {
                iter.seek( recSerializer.keyBytes( startKey ) );
            }
            for ( ; iter.isValid(); iter.next() )
            {
                byte[] key = iter.key();
                // Don't stop after reaching Integer.Max_VALUE as there may be
                // negative ids after Integer overflow
                if ( null != endKey && keyCompare( key, endKeyBytes ) > 0 )
                {
                    break;
                }
                byte[] valueBytes = iter.value();
                c.accept( recSerializer.toIndexEntry( key, valueBytes ) );
                processed++;
            }
            return processed;
        }
    }

    @Override
    public K maxKey()
    {
        try (RocksIterator iter = db.newIterator( new ReadOptions() ))
        {
            for ( iter.seekToLast(); iter.isValid(); )
            {
                byte[] keyBytes = iter.key();
                return recSerializer.key( keyBytes );
            }
        }
        return null;
    }

    @Override
    public void close()
    {
        log.info( "closing metric name index database [" + dbName + "]" );
        closeQuietly( db );
        log.info( "closed metric name index database [" + dbName + "]" );
    }

    @Override
    public String getName()
    {
        return dbName;
    }

    @Override
    public File getDbDir() {
        return dbDir;
    }

    @Override
    public R dbGet( K key )
    {
        byte[] keyBytes = recSerializer.keyBytes( key );
        byte[] valueBytes = dbGet( keyBytes );
        if ( valueBytes != null )
        {
            return recSerializer.toIndexEntry( key, valueBytes );
        }
        else
        {
            return null;
        }
    }

    @Override
    public void dbDelete( K key )
    {
        if (rocksdbReadonly) {
            throw new UnsupportedOperationException("Method dbDelete is not supported for readonly mode");
        }
        byte[] keyBytes = recSerializer.keyBytes( key );
        dbDelete( keyBytes );
    }

    @Override
    public void dbPut( R e )
    {
        if (rocksdbReadonly) {
            throw new UnsupportedOperationException("Method dbPut is not supported for readonly mode");
        }
        byte[] key = recSerializer.keyBytes( e.key() );
        byte[] value = recSerializer.valueBytes( e );

        dbPut( key, value );
    }

    @Override
    public String dbGetProperty(String property) {
        try {
            return db.getProperty("rocksdb." + property);
        } catch (RocksDBException e) {
            log.error("Failed to retrieve property {} - {}", property, e.getMessage(), e);
            return null;
        }
    }

    private void dbPut( byte[] k, byte[] v )
    {
        if (rocksdbReadonly) {
            throw new UnsupportedOperationException("Method dbPut is not supported for readonly mode");
        }
        try (Timer.Context ignored = writeTimer.time())
        {
            db.put( k, v );
        }
        catch ( RocksDBException e )
        {
            throw new RuntimeException( e );
        }
    }

    private byte[] dbGet( byte[] k )
    {
        try (Timer.Context ignored = readTimer.time())
        {
            return db.get( k );
        }
        catch ( RocksDBException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void dbDelete( byte[] keyBytes )
    {
        if (rocksdbReadonly) {
            throw new UnsupportedOperationException("Method dbDelete is not supported for readonly mode");
        }
        try (Timer.Context ignored = delTimer.time())
        {
            db.delete( keyBytes );
        }
        catch ( RocksDBException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void closeQuietly( RocksDB db )
    {
        if (rocksdbReadonly) {
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
                log.error( "Error while closing database [" + dbName + "].", e );
            }
        }
    }

    private String dbWriteTimerName( String dbName )
    {
        return "db." + dbName + ".write.time";
    }

    private String dbReadTimerName( String dbName )
    {
        return "db." + dbName + ".read.time";
    }

    private String dbDeleteTimerName( String dbName )
    {
        return "db." + dbName + ".delete.time";
    }

    private static class SyncPrimaryDbTask implements Runnable {
        private final RocksDB rocksDB;

        private final File dbDir;

        private SyncPrimaryDbTask(RocksDB rocksDB, File dbDir) {
            this.rocksDB = rocksDB;
            this.dbDir = dbDir;
        }

        @Override
        public void run() {
            try {
                log.info("Start syncing with primary DB {}", dbDir.getAbsolutePath());
                rocksDB.tryCatchUpWithPrimary();
                log.info("Completed syncing with primary DB {}", dbDir.getAbsolutePath());
            } catch (RocksDBException e) {
                log.error("Failed to sync with primary DB {} - {}", dbDir.getAbsolutePath(), e.getMessage(), e);
            }
        }
    }
}
