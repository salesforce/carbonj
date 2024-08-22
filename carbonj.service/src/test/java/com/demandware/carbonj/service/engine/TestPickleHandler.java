/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.codahale.metrics.MetricRegistry;
import net.razorvine.pickle.Pickler;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPickleHandler {

    @Test
    public void testStringMetricValueSupport() throws Exception {
        int epochTimeInSecs = TimeSource.defaultTimeSource().getEpochSecond();

        Object[] timeAndDoubleValue = new Object[] {epochTimeInSecs, 1.0d};
        Object[] metricObject1 = new Object[] {"test.metric1", timeAndDoubleValue};

        Object[] timeAndStringValue = new Object[] {epochTimeInSecs, "3.0"};
        Object[] metricObject2 = new Object[] {"test.metric2", timeAndStringValue};

        List<Object> pickleDs = new ArrayList<>();
        pickleDs.add(metricObject1);
        pickleDs.add(metricObject2);

        byte[] pickledBytes = new Pickler().dumps(pickleDs);
        MetricsConsumer consumer = new MetricsConsumer();
        MetricRegistry metricRegistry = mock( MetricRegistry.class);
        when(metricRegistry.counter(
                anyString()) ).thenReturn( mock( Counter.class ) );
        PickleProtocolHandler pickleProtocolHandler = new PickleProtocolHandler(metricRegistry, consumer);
        pickleProtocolHandler.handle(new ByteArrayInputStream(pickledBytes));

        assertEquals(2, consumer.metrics.size());

        DataPoint dataPoint = consumer.metrics.get(0);
        assertEquals(1.0, dataPoint.val, 0.001);
        assertEquals("test.metric1", dataPoint.name);

        dataPoint = consumer.metrics.get(1);
        assertEquals(3.0, dataPoint.val, 0.001);
        assertEquals("test.metric2", dataPoint.name);
    }

    private static class MetricsConsumer implements Consumer<DataPoint> {

        private final List<DataPoint> metrics = new ArrayList<>();

        @Override
        public void accept(DataPoint dataPoint) {
            metrics.add(dataPoint);
        }
    }
}
