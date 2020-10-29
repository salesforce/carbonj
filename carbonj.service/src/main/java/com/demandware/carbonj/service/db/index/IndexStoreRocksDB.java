/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;
import java.io.PrintWriter;
import java.util.function.Consumer;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.primitives.UnsignedBytes;

class IndexStoreRocksDB<K, R extends Record<K>>
    implements IndexStore<K, R>
{
    private static final Logger log = LoggerFactory.getLogger( IndexStoreRocksDB.class );

    final private String dbName;

    final private File dbDir;

    private RocksDB db;

    final private RecordSerializer<K, R> recSerializer;

    private Timer writeTimer;

    private Timer readTimer;

    private Timer delTimer;

    public IndexStoreRocksDB(MetricRegistry metricRegistry, String dbName, File dbDir, RecordSerializer<K, R> recSerializer )
    {
        this.dbName = Preconditions.checkNotNull( dbName );
        this.dbDir = Preconditions.checkNotNull( dbDir );
        this.recSerializer = recSerializer;
        this.writeTimer = metricRegistry.timer( dbWriteTimerName( dbName ) );
        this.readTimer = metricRegistry.timer( dbReadTimerName( dbName ) );
        this.delTimer = metricRegistry.timer( dbDeleteTimerName( dbName ) );
    }

    @Override
    public void dumpStats()
    {
    }

    @Override
    public void open()
    {
        log.info( "Opening RocksDB metric index store in [" + dbDir + "]" );

        TtlDB.loadLibrary();

        Options options =
            new Options().setCreateIfMissing( true ).setCompressionType( CompressionType.SNAPPY_COMPRESSION );

        try
        {
            this.db = RocksDB.open( options, dbDir.getAbsolutePath() );
        }
        catch ( RocksDBException e )
        {
            throw Throwables.propagate( e );
        }
    }

    @Override
    public void dump( PrintWriter pw )
    {
        scan( null, null, r -> pw.println( r ) );
    }

    private static int keyCompare( byte[] keyBytes1, byte[] keyBytes2 )
    {
        return UnsignedBytes.lexicographicalComparator().compare( keyBytes1, keyBytes2 );
    }

    @Override
    public int scan( K startKey, K endKey, Consumer<R> c )
    {
        int processed = 0;
        byte[] endKeyBytes = null == endKey ? null : recSerializer.keyBytes( endKey );
        try (RocksIterator iter = db.newIterator( new ReadOptions() ))
        {
            if ( null == startKey )
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
                if ( null != endKey && keyCompare( key, endKeyBytes ) >= 0 )
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
        byte[] keyBytes = recSerializer.keyBytes( key );
        dbDelete( keyBytes );
    }

    @Override
    public void dbPut( R e )
    {
        byte[] key = recSerializer.keyBytes( e.key() );
        byte[] value = recSerializer.valueBytes( e );

        dbPut( key, value );
    }

    private void dbPut( byte[] k, byte[] v )
    {
        final Timer.Context timerContext = writeTimer.time();
        try
        {
            db.put( k, v );
        }
        catch ( RocksDBException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            timerContext.stop();
        }
    }

    private byte[] dbGet( byte[] k )
    {
        final Timer.Context timerContext = readTimer.time();
        try
        {
            return db.get( k );
        }
        catch ( RocksDBException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            timerContext.stop();
        }
    }

    private void dbDelete( byte[] keyBytes )
    {
        final Timer.Context timerContext = delTimer.time();
        try
        {
            db.remove( keyBytes );
        }
        catch ( RocksDBException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            timerContext.stop();
        }
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
}
