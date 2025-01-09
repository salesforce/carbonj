/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.*;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.engine.GraphitePickler;
import com.demandware.carbonj.service.engine.Query;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStreamSeriesBatchMetricsTask {
    @Test
    public void test() throws Exception {
        MetricRegistry metricRegistry  = new MetricRegistry();
        DatabaseMetrics databaseMetrics = new DatabaseMetrics(metricRegistry);
        Metric metric = new Metric("foo.bar", 1, null, List.of(RetentionPolicy.getInstance("60s:24h")), new ArrayList<>());
        StreamSeriesBatchMetricsTask streamSeriesBatchMetricsTask = new StreamSeriesBatchMetricsTask(
                new MockDataPointStore(), List.of(metric), new Query("foo.bar", 0, 60, 0, 1000), new GraphitePickler(System.out), new QueryDurations());
        BatchStats batchStats = streamSeriesBatchMetricsTask.call();
        assertEquals(2, batchStats.noOfDataPoints);
        assertEquals(String.format("Task [%d] for pattern [foo.bar], from=0, until=60, now=0, size=1", streamSeriesBatchMetricsTask.hashCode()), streamSeriesBatchMetricsTask.toString());
    }

    private static class MockDataPointStore implements DataPointStore {

        @Override
        public DataPointImportResults importDataPoints(String dbName, List<DataPoint> dataPoint, int maxAllowedImportErrors) {
            return null;
        }

        @Override
        public void insertDataPoints(DataPoints points) {

        }

        @Override
        public void importDataPoints(String dbName, DataPoints points) {

        }

        @Override
        public long delete(String archive, int ts) {
            return 0;
        }

        @Override
        public void delete(List<Metric> m) {

        }

        @Override
        public Series getSeries(Metric metric, int from, int until, int now) {
            return new Series("foo.bar", from, until, 60, List.of(1.0, 2.0));
        }

        @Override
        public List<DataPointValue> getValues(RetentionPolicy archivePolicy, long metricId, int from, int to) {
            return List.of();
        }

        @Override
        public void open() {

        }

        @Override
        public void close() {

        }

        @Override
        public DataPointValue getFirst(RetentionPolicy instanceForDbName, long id, int from, int to) {
            return null;
        }

        @Override
        public void dumpStats() {

        }
    }
}
