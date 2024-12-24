/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.BaseTest;
import com.demandware.carbonj.service.db.index.IndexUtils;
import com.demandware.carbonj.service.db.model.*;
import com.demandware.carbonj.service.engine.AggregationMethod;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestIntervalProcessors extends BaseTest {

    private static final String SORTED_STAGING_FILE_NAME = "30m2y-1559811600.1.s";

    @Test
    @Disabled("This test requires a staging file that is 700M big. This is gonna be a hard sell for open-sourcing..")
    public void testFileReadingTime() {

        String testFile = ClassLoader.getSystemResource(SORTED_STAGING_FILE_NAME).getFile();
        IntervalProcessorTaskFactory mockTaskFactory = mockTaskFactory();
        IntervalProcessors intervalProcessors = new IntervalProcessors(metricRegistry,30000, 1000000, 4, mockTaskFactory, 100);
        MetricProvider mockMetricProvider = mockMetricProvider();
        IntervalProcessors.Stats stats = intervalProcessors.processFile(new SortedStagingFile(new File(testFile), mockMetricProvider));

        assertEquals(54622104, stats.nLines);
        assertEquals(11518592, stats.nRecords);
    }

    private MetricProvider mockMetricProvider() {
        return new SimpleMetricProvider();
    }

    private IntervalProcessorTaskFactory mockTaskFactory() {
        return new SimpleIntervalProcessorTaskFactory();
    }

    private static class SimpleIntervalProcessorTaskFactory implements IntervalProcessorTaskFactory {

        @Override
        public Runnable create(List<IntervalValues> intervals) {
            return new Runnable () {
                @Override
                public void run() {

                }
            };
        }
    }

    private static class SimpleMetricProvider implements MetricProvider {

        @Override
        public Metric forId(long metricId) {
            return new Metric("name", 1,
                    new AggregationPolicy(AggregationMethod.AVG, 1, null),
                    Collections.emptyList(),
                    Collections.emptyList());
        }

        @Override
        public Metric forName(String metricName) {
            return new Metric(metricName, 1,
                    new AggregationPolicy(AggregationMethod.AVG, 1, null),
                    Collections.emptyList(),
                    Collections.emptyList());
        }
    }

    @Test
    public void test() throws Exception {
        MetricIndex metricIndex = IndexUtils.metricIndex(new File("/tmp/testdb"), true);
        metricIndex.open();
        Metric metric1 = metricIndex.createLeafMetric("pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io");
        assertEquals(2, metric1.id);
        Metric metric2 = metricIndex.createLeafMetric("POD276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.a2su9qja883gs.1965276335.sqlmonitor.elapsed_time");
        assertEquals(3, metric2.id);
        DataPointStore dataPointStore = DataPointStoreUtils.createDataPointStore(metricRegistry, new File("/tmp/testdb"), true, metricIndex);
        File staging = new File("/tmp/5m7d-1734989700-9.1.s");
        List<String> lines = List.of(
                "11987976699 2 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io",
                "11987976699 3 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io",
                "11987976699 1 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io",
                "11987984189 5239330 POD276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.a2su9qja883gs.1965276335.sqlmonitor.elapsed_time",
                "11987984189 5239330 POD276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.a2su9qja883gs.1965276335.sqlmonitor.elapsed_time");
        FileUtils.writeLines(staging, lines);
        SortedStagingFile sortedStagingFile = new SortedStagingFile(staging, metricIndex);
        IntervalProcessors intervalProcessors = new IntervalProcessors(metricRegistry,30000, 1000000, 4, new IntervalProcessorTaskFactoryImpl(dataPointStore), 100);
        IntervalProcessors.Stats stats = intervalProcessors.processFile(sortedStagingFile);
        assertEquals(5, stats.nLines);
        assertEquals(2, stats.nRecords);
        metricIndex.close();
    }
}
