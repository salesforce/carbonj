/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.demandware.carbonj.service.BaseTest;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.demandware.carbonj.service.events.NoOpLogger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@Disabled
public class TestTimeSeries extends BaseTest {

    @Test
    public void testEstimatedNumberOfDataPoints() {
        MetricIndex nameIndex = mock(MetricIndex.class);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1,1 ,
                TimeUnit.HOURS, new ArrayBlockingQueue<>(10));
        TimeSeriesStoreImpl timeSeriesStore = new TimeSeriesStoreImpl(metricRegistry, nameIndex, new NoOpLogger<>(),
                threadPoolExecutor, threadPoolExecutor,
                threadPoolExecutor, mock(DataPointStore.class), new DatabaseMetrics(metricRegistry), false,
                100, false, null, 1,
                "DoesNotExist", false);

        RetentionPolicy.getInstance("60s:24h");
        RetentionPolicy.getInstance("5m:7d");
        RetentionPolicy.getInstance("30m:2y");

        int now = TimeSource.defaultTimeSource().getEpochSecond();
        List<Metric> metrics = new ArrayList<>();

        long numberOfDataPoints = timeSeriesStore.getEstimatedNumberOfDataPoints(
                (int) (now - TimeUnit.HOURS.toSeconds(2)),
                (int) (now - TimeUnit.HOURS.toSeconds(1)), now, metrics);

        assertEquals(0, numberOfDataPoints);

        metrics.add(mock(Metric.class));

        numberOfDataPoints = timeSeriesStore.getEstimatedNumberOfDataPoints(
                (int) (now - TimeUnit.HOURS.toSeconds(2)),
                (int) (now - TimeUnit.HOURS.toSeconds(1)), now, metrics);
        assertEquals(60, numberOfDataPoints);

        numberOfDataPoints = timeSeriesStore.getEstimatedNumberOfDataPoints(
                (int) (now - TimeUnit.HOURS.toSeconds(36)),
                (int) (now - TimeUnit.HOURS.toSeconds(2)), now, metrics);
        assertEquals(34 * 12, numberOfDataPoints);

        numberOfDataPoints = timeSeriesStore.getEstimatedNumberOfDataPoints(
                (int) (now - TimeUnit.DAYS.toSeconds(400)),
                (int) (now - TimeUnit.HOURS.toSeconds(2)), now, metrics);
        assertEquals(((400 * 24) - 2) * 2, numberOfDataPoints);
    }
}
