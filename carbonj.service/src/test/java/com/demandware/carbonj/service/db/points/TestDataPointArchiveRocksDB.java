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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
