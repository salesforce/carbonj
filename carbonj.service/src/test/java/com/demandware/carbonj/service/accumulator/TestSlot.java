/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSlot {
    @Test
    public void testSlot() {
        MetricRegistry metricRegistry = new MetricRegistry();
        Timer aggregatorFlushTimer = metricRegistry.timer(MetricRegistry.name("aggregator", "slotFlushTimer"));
        Meter flushedAggregates = metricRegistry.meter(MetricRegistry.name("aggregator", "aggregates"));
        Meter createdSlots = metricRegistry.meter(MetricRegistry.name("aggregator", "slotCreated"));
        AtomicInteger counter = new AtomicInteger();
        Slot slot = new Slot(60, new CountingLatePointLogger(counter), 1, aggregatorFlushTimer, flushedAggregates, createdSlots);
        MetricAggregate metricAggregate = new MetricAggregate("foo.bar.aggregated", MetricAggregationMethod.LATENCY, false);
        slot.apply(metricAggregate, new DataPoint("foo.bar", 123, 90, false), 90);
        assertEquals(1, slot.size());
        assertEquals(60, slot.getTs());
        slot.close(dataPoints -> {System.out.println(dataPoints.get(0));});
        assertEquals(4, flushedAggregates.getCount());
        assertEquals(0, counter.get());
        slot.apply(metricAggregate, new DataPoint("foo.bar", 123, 90, false), 90);
        assertEquals(1, counter.get());
    }
}
