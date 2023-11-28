/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.demandware.carbonj.service.BaseTest;
import com.demandware.carbonj.service.db.index.IndexUtils;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.points.DataPointStoreUtils;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.engine.Query;
import com.demandware.carbonj.service.events.NoOpLogger;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class Test60s30dTimeSeries extends BaseTest {

    private static final boolean longId = true;

    @Test
    public void test60s30dTimeSeries() throws Exception {
        long currentTime = Clock.systemUTC().millis();
        File dbDirFile = new File("/tmp/" + currentTime);
        Assert.assertTrue(dbDirFile.mkdirs());
        File metricStoreConfFile = new File(dbDirFile, "metric-store.conf");
        List<String> metricStoreConfigs = new ArrayList<>();
        metricStoreConfigs.add("metrics.store.retentions=60s:30d");
        metricStoreConfigs.add("metrics.store.lowerResolutionArchives.enabled=false");
        FileUtils.writeLines(metricStoreConfFile, metricStoreConfigs);
        MetricIndex metricIndex = IndexUtils.metricIndex(dbDirFile, longId, metricStoreConfFile.getAbsolutePath());
        metricIndex.open();
        DataPointStore dataPointStore = DataPointStoreUtils.createDataPointStore(metricRegistry, dbDirFile, longId, metricIndex);
        dataPointStore.open();

        RetentionPolicy.getInstance("60s:30d");
        metricIndex.createLeafMetric("a.b.c");

        TimeSeriesStore timeSeriesStore = new TimeSeriesStoreImpl(metricRegistry, metricIndex, new NoOpLogger<>(),
                TimeSeriesStoreImpl.newMainTaskQueue(1, 1),
                TimeSeriesStoreImpl.newHeavyQueryTaskQueue(1, 1),
                TimeSeriesStoreImpl.newSerialTaskQueue(1),
                dataPointStore, new DatabaseMetrics(metricRegistry), false,
                100, false, null, 1,
                "DoesNotExist", longId);

        List<DataPoint> points = new ArrayList<>();
        points.add(new DataPoint("a.b.c", 123, (int)(currentTime / 1000)));
        timeSeriesStore.accept(new DataPoints(points));
        Metric metric = timeSeriesStore.getMetric("a.b.c");
        Assert.assertEquals(2, metric.id);
        Assert.assertEquals(1, metric.getRetentionPolicies().size());
        Assert.assertTrue(metric.getRetentionPolicies().get(0).is60s30d());
        Assert.assertTrue(new File(dbDirFile, "60s30d").exists());
        Assert.assertTrue(new File(new File(dbDirFile, "60s30d"), "LOG").exists());
        long now = Clock.systemUTC().millis();
        List<Series> series = timeSeriesStore.fetchSeriesData(new Query("a.b.c", (int)(currentTime / 1000),
                (int)(currentTime / 1000) + 10, (int)(now / 1000), now));
        Assert.assertEquals(1, series.size());
        Assert.assertEquals("a.b.c", series.get(0).name);
        Assert.assertEquals(60, series.get(0).step);
        Assert.assertEquals(1, series.get(0).values.size());
        Assert.assertEquals(123, series.get(0).values.get(0).intValue());

        dataPointStore.close();
        metricIndex.close();
    }
}
