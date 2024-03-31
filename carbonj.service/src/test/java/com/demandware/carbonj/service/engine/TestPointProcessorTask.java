/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPointProcessorTask {
    private static final String SERIES_NAME = "client.request_response.total.time.p99";

    private static final List<DataPoint> TEST_POINT = Collections.singletonList(new DataPoint(SERIES_NAME, 1, 1));

    @Test
    public void testSinglePointBlocked() {
        MetricList blocked = getMetricList(true);
        MetricList allowOnly = getMetricList(false);

        PointProcessorTask task = getTask(blocked, allowOnly, false);

        task.process(TEST_POINT);
    }

    @Test
    public void testSinglePointAllowed() {
        MetricList blocked = getMetricList(false);
        MetricList allowOnly = getMetricList(true);

        PointProcessorTask task = getTask(blocked, allowOnly, true);

        task.process(TEST_POINT);
    }

    @Test
    public void testSinglePointAllowedAndBlocked() {
        // this could be "\.stddev" - filtering a leaf not necessary for upstream
        MetricList blocked = getMetricList(true);
        // this could be a list of top level trees allowed for forwarding, like "client|backend|infra"
        MetricList allowOnly = getMetricList(true);

        PointProcessorTask task = getTask(blocked, allowOnly, false);

        task.process(TEST_POINT);
    }

    private PointProcessorTask getTask(MetricList blocked, MetricList allowOnly, boolean expectPointsValid) {
        return new PointProcessorTask(new MetricRegistry(), TEST_POINT, blocked, allowOnly, null, false,
                getAcceptAllFilter(), validatingConsumer(expectPointsValid), mockRelay(), mockNsCounter(), false, new ConcurrentLinkedQueue<>());
    }

    private MetricList getMetricList(boolean matchesTestSeries) {
        MetricList list = mock(MetricList.class);
        when(list.match(same(SERIES_NAME))).thenReturn(matchesTestSeries);
        return list;
    }

    private Consumer<DataPoints> validatingConsumer(boolean expectValidPoints) {
        return dataPoints -> {
            for (int i = 0; i < dataPoints.size(); i++) {
                DataPoint point = dataPoints.get(i);
                Assert.assertEquals("Datapoint (" + point.toString() + ") validity:" , expectValidPoints, point.isValid());
            }
        };
    }

    private PointFilter getAcceptAllFilter() {
        PointFilter filter = mock(PointFilter.class);
        when(filter.accept(any())).thenReturn(true);
        return filter;
    }

    private Relay mockRelay() {
        return mock(Relay.class);
    }

    private NamespaceCounter mockNsCounter() {
        return mock(NamespaceCounter.class);
    }
}
