/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.DataPointImportResults;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.QueryCachePolicy;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class TestDataPointStoreImpl {
    @Test
    public void test() {
        MetricRegistry metricRegistry  = new MetricRegistry();
        RocksDBConfig rocksDBConfig = new RocksDBConfig();
        DatabaseMetrics databaseMetrics = new DatabaseMetrics(metricRegistry);
        DataPointArchiveFactory dataPointArchiveFactory = new DataPointArchiveFactory(metricRegistry, new File("/tmp/testdb"), rocksDBConfig, true);
        DataPointStore dataPointStore = new DataPointStoreImpl(metricRegistry, dataPointArchiveFactory, databaseMetrics, null,
                true, 1, 0,
                new QueryCachePolicy(false, false, false, false), new Predicate<String>() {
            @Override
            public boolean test(String s) {
                return !s.endsWith(".invalid");
            }
        });
        RetentionPolicy retentionPolicy = RetentionPolicy.getInstance("60s:24h");
        DataPoint dataPoint = new DataPoint("foo.bar", 123, 60);
        dataPoint.setMetricId(12345L);
        DataPointImportResults dataPointImportResults = dataPointStore.importDataPoints("60s24h", List.of(dataPoint), 0);
        assertEquals(1, dataPointImportResults.received);
        assertEquals(1, dataPointImportResults.expired);

        dataPoint = new DataPoint("foo.bar", 123, 61);
        dataPoint.setMetricId(12345L);
        try {
            dataPointStore.importDataPoints("60s24h", List.of(dataPoint), 0);
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            assertEquals("Number of errors [1] exceeds specified maxAllowedImportErrors [0] for the request", e.getMessage());
        }

        int current = (int) (System.currentTimeMillis() / 1000);
        dataPoint = new DataPoint("foo.bar", 123, retentionPolicy.interval(current));
        dataPoint.setMetricId(12345L);
        dataPointImportResults = dataPointStore.importDataPoints("60s24h", List.of(dataPoint), 0);
        assertEquals(1, dataPointImportResults.received);
        assertEquals(1, dataPointImportResults.saved);

        Metric metric = new Metric("foo.bar", 12345L, null, List.of(retentionPolicy), new ArrayList<>());
        Series series = dataPointStore.getSeries(metric, retentionPolicy.interval(current), retentionPolicy.interval(current) + 60, (int) (System.currentTimeMillis() / 1000));
        assertEquals(2, series.values.size());
        assertEquals(123, series.values.get(0));
        assertNull(series.values.get(1));

        assertEquals(1, dataPointStore.delete("60s24h", retentionPolicy.interval(current)));

        metric = new Metric("foo.bar.invalid", 12345L, null, List.of(retentionPolicy), new ArrayList<>());
        series = dataPointStore.getSeries(metric, retentionPolicy.interval(current), retentionPolicy.interval(current) + 60, (int) (System.currentTimeMillis() / 1000));
        assertEquals(2, series.values.size());
        assertNull(series.values.get(0));
        assertNull(series.values.get(1));

        // Negative tests
        dataPoint = new DataPoint("foo.bar", 123, 0);
        DataPoints dataPoints = new DataPoints(List.of(dataPoint));
        metric = new Metric("foo.bar", 12345L, null, List.of(retentionPolicy), new ArrayList<>());
        dataPoints.assignMetric(0, metric, retentionPolicy);
        dataPointStore.insertDataPoints(dataPoints);

        dataPoint = new DataPoint("foo.bar", 123, 60);
        dataPoint.setMetricId(DataPoint.UNKNOWN_ID);
        dataPoints = new DataPoints(List.of(dataPoint));
        metric = new Metric("foo.bar", DataPoint.UNKNOWN_ID, null, List.of(retentionPolicy), new ArrayList<>());
        dataPoints.assignMetric(0, metric, retentionPolicy);
        dataPointStore.insertDataPoints(dataPoints);

        dataPointStore.close();
    }
}
