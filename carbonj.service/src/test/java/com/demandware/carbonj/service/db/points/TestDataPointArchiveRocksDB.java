/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDataPointArchiveRocksDB {
    @Test
    public void test() {
        MetricRegistry metricRegistry = new MetricRegistry();
        RocksDBConfig rocksDBConfig = new RocksDBConfig();
        DataPointArchiveRocksDB dataPointArchiveRocksDB = new DataPointArchiveRocksDB(
                metricRegistry, "60s24h", RetentionPolicy.getInstance("60s:24h"), new File("/tmp/testdb"), rocksDBConfig, true);
        dataPointArchiveRocksDB.open();
        dataPointArchiveRocksDB.put(12345L, 60, 123.45);
        List<DataPointValue> dataPointValueList = dataPointArchiveRocksDB.getDataPoints(12345L, 0, 60);
        assertEquals(1, dataPointValueList.size());
        DataPointValue dataPointValue = dataPointValueList.get(0);
        assertEquals(60, dataPointValue.ts);
        assertEquals(123.45, dataPointValue.val);
        dataPointArchiveRocksDB.delete(60);
        dataPointValueList = dataPointArchiveRocksDB.getDataPoints(12345L, 0, 60);
        assertTrue(dataPointValueList.isEmpty());
        DataPoint dataPoint = new DataPoint("foo.bar", 123, 0);
        assertEquals(0, dataPointArchiveRocksDB.put(new DataPoints(List.of(dataPoint))));
        dataPoint = new DataPoint("foo.bar", 123, 60);
        dataPoint.setMetricId(DataPoint.UNKNOWN_ID);
        assertEquals(0, dataPointArchiveRocksDB.put(new DataPoints(List.of(dataPoint))));
        dataPoint.setMetricId(12345L);
        assertEquals(0, dataPointArchiveRocksDB.put(new DataPoints(List.of(dataPoint))));
        dataPoint = new DataPoint("foo.bar", 123, (int) (System.currentTimeMillis() / 1000));
        dataPoint.setMetricId(12345L);
        assertEquals(0, dataPointArchiveRocksDB.put(new DataPoints(List.of(dataPoint))));
        dataPointArchiveRocksDB.close();
    }

    @Test
    public void registersIteratorAndRocksDbDiagnostics() {
        MetricRegistry metricRegistry = new MetricRegistry();
        DataPointArchiveRocksDB archive = new DataPointArchiveRocksDB(
                metricRegistry, "60s24h", RetentionPolicy.getInstance("60s:24h"), new File("/tmp/testdb-diagnostics"),
                new RocksDBConfig(), true);

        assertNotNull(metricRegistry.getMeters().get("db.60s24h.rocksdb.iterator.opened"));
        assertNotNull(metricRegistry.getMeters().get("db.60s24h.rocksdb.iterator.closed"));
        assertNotNull(metricRegistry.getMeters().get("db.60s24h.rocksdb.iterator.closeRejected"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.iterator.active"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.cleaner.queueSize"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.cleaner.queueRemainingCapacity"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.cleaner.activeThreads"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.numLiveVersions"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.liveSstFilesSize"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.totalSstFilesSize"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.estimateLiveDataSize"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.pendingCompactionBytes"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.runningCompactions"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.runningFlushes"));
        assertNotNull(metricRegistry.getGauges().get("db.60s24h.rocksdb.immutableMemtables"));

        archive.open();
        assertTrue((Long) metricRegistry.getGauges().get("db.60s24h.rocksdb.totalSstFilesSize").getValue() >= 0);
        archive.close();
    }

    @Test
    public void closesIteratorWhenCleanerQueueIsFull() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        RocksDBConfig rocksDBConfig = new RocksDBConfig();
        rocksDBConfig.objectCleanerQueueSize = 1;
        DataPointArchiveRocksDB archive = new DataPointArchiveRocksDB(
                metricRegistry, "60s24h", RetentionPolicy.getInstance("60s:24h"), new File("/tmp/testdb-cleaner-full"),
                rocksDBConfig, true);
        archive.open();
        archive.put(12345L, 60, 123.45);

        ThreadPoolExecutor cleaner = cleanerOf( archive );
        CountDownLatch releaseCleaner = new CountDownLatch( 1 );
        cleaner.execute( () -> await( releaseCleaner ) );
        cleaner.execute( () -> { } );

        assertEquals( 1, archive.getDataPoints( 12345L, 0, 60 ).size() );
        assertEquals( 1L, metricRegistry.getMeters().get( "db.60s24h.rocksdb.iterator.closeRejected" ).getCount() );
        assertEquals( 1L, metricRegistry.getMeters().get( "db.60s24h.rocksdb.iterator.closed" ).getCount() );
        assertEquals( 0L, metricRegistry.getGauges().get( "db.60s24h.rocksdb.iterator.active" ).getValue() );

        releaseCleaner.countDown();
        archive.close();
    }

    private ThreadPoolExecutor cleanerOf( DataPointArchiveRocksDB archive ) throws Exception {
        Field cleanerField = DataPointArchiveRocksDB.class.getDeclaredField( "cleaner" );
        cleanerField.setAccessible( true );
        return (ThreadPoolExecutor) cleanerField.get( archive );
    }

    private void await( CountDownLatch latch ) {
        try {
            latch.await( 10, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new AssertionError( e );
        }
    }
}
