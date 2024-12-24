/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.index.IndexUtils;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.IntervalValues;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestIntervalProcessorImpl {
    @Test
    public void test() throws Exception {
        MetricRegistry metricRegistry  = new MetricRegistry();
        MetricIndex metricIndex = IndexUtils.metricIndex(new File("/tmp/testdb"), true);
        metricIndex.open();
        Metric metric1 = metricIndex.createLeafMetric("pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io");
        assertEquals(2, metric1.id);
        DataPointStore dataPointStore = DataPointStoreUtils.createDataPointStore(metricRegistry, new File("/tmp/testdb"), true, metricIndex);
        IntervalProcessorImpl intervalProcessor = new IntervalProcessorImpl(metricRegistry, "60s24h", 1, 1, 1, new IntervalProcessorTaskFactoryImpl(dataPointStore), 100);
        assertEquals("60s24h", intervalProcessor.getDbName());
        assertEquals("IntervalProcessor{, dbName='60s24h'}", intervalProcessor.toString());
        Thread thread = new Thread(intervalProcessor);
        thread.start();
        IntervalValues intervalValues = new IntervalValues(metric1, List.of(1.0, 2.0), 60, "60s24h");
        intervalProcessor.put(intervalValues);
        Thread.sleep(500);
        intervalProcessor.close();
        thread.join();
        metricIndex.close();
    }
}
