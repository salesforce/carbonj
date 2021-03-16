/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.BaseTest;
import com.demandware.carbonj.service.db.model.*;
import com.demandware.carbonj.service.engine.AggregationMethod;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.Collections;
import java.util.List;

@RunWith(JUnit4.class)
public class TestIntervalProcessors extends BaseTest {

    private static final String SORTED_STAGING_FILE_NAME = "30m2y-1559811600.1.s";

    @Test
    @Ignore("This test requires a staging file that is 700M big. This is gonna be a hard sell for open-sourcing..")
    public void testFileReadingTime() {

        String testFile = ClassLoader.getSystemResource(SORTED_STAGING_FILE_NAME).getFile();
        IntervalProcessorTaskFactory mockTaskFactory = mockTaskFactory();
        IntervalProcessors intervalProcessors = new IntervalProcessors(metricRegistry,30000, 1000000, 4, mockTaskFactory, 100);
        MetricProvider mockMetricProvider = mockMetricProvider();
        IntervalProcessors.Stats stats = intervalProcessors.processFile(new SortedStagingFile(new File(testFile), mockMetricProvider));

        Assert.assertEquals(54622104, stats.nLines);
        Assert.assertEquals(11518592, stats.nRecords);
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
    }
}
