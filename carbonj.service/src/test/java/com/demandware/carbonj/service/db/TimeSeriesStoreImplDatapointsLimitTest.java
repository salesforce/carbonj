/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.TooManyDatapointsFoundException;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.events.NoOpLogger;
import com.demandware.carbonj.service.engine.Query;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyBoolean;

public class TimeSeriesStoreImplDatapointsLimitTest {

    @Test
    public void incrementsMeterWhenTooManyDatapointsAreMatched() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        DatabaseMetrics dbMetrics = new DatabaseMetrics(metricRegistry);

        // Build a MetricIndex that returns 1 leaf metric with a long retention so estimated points is large
        MetricIndex nameIndex = mock(MetricIndex.class);
        Metric m = mock(Metric.class);
        when(m.isLeaf()).thenReturn(true);
        // Use 30m:2y retention so each series can be huge; we'll also set the threshold very low
        when(m.getRetentionPolicies()).thenReturn(RetentionPolicy.getPolicyList("30m:2y"));
        when(nameIndex.findMetrics(any(String.class), anyBoolean(), anyBoolean(), anyBoolean()))
                .thenReturn(List.of(m));

        ThreadPoolExecutor main = null;
        ThreadPoolExecutor heavy = null;
        ThreadPoolExecutor serial = null;

        // Create store with a non-existent config file, then override thresholds via reflection-free path:
        // Create a temp properties file with a very low datapoints threshold to force the exception
        File tmpProps = null;

        try {
            main = TimeSeriesStoreImpl.newMainTaskQueue(1, 10);
            heavy = TimeSeriesStoreImpl.newMainTaskQueue(1, 10);
            serial = TimeSeriesStoreImpl.newSerialTaskQueue(10);

            tmpProps = File.createTempFile("tsstore", ".properties");
            try (FileWriter fw = new FileWriter(tmpProps)) {
                fw.write("metrics.store.maxDataPointsPerRequest=10\n");
            }

            TimeSeriesStoreImpl store = new TimeSeriesStoreImpl(metricRegistry, nameIndex, new NoOpLogger<>(),
                    main, heavy, serial,
                    mock(com.demandware.carbonj.service.db.model.DataPointStore.class), dbMetrics,
                    true, 10, false, new File("/tmp/idx.out"), 10,
                    tmpProps.getAbsolutePath(), false);

            // Manually set thresholds via internal fields to avoid filesystem config
            // dataPointsThreshold is private, but enforced via selectThreadPoolExecutor; we can choose query that exceeds
            int now = (int) (System.currentTimeMillis() / 1000);
            Query q = new Query("foo", now - 365 * 24 * 3600, now, now, System.currentTimeMillis());

            long before = metricRegistry.meter("db.datapointsLimitExceeded").getCount();

            assertThrows(TooManyDatapointsFoundException.class, () -> store.fetchSeriesData(q));

            long after = metricRegistry.meter("db.datapointsLimitExceeded").getCount();
            // Ensure the meter was incremented once
            assertEquals(before + 1, after);
        } finally {
            if (main != null) {
                main.shutdownNow();
            }
            if (heavy != null) {
                heavy.shutdownNow();
            }
            if (serial != null) {
                serial.shutdownNow();
            }
            if (tmpProps != null) {
                //noinspection ResultOfMethodCallIgnored
                tmpProps.delete();
            }
        }
    }
}


