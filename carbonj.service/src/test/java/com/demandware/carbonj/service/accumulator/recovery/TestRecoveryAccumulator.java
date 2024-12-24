/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator.recovery;

import com.demandware.carbonj.service.BaseTest;
import com.demandware.carbonj.service.accumulator.CountingLatePointLogger;
import com.demandware.carbonj.service.accumulator.DefaultSlotStrategy;
import com.demandware.carbonj.service.accumulator.MetricAggregate;
import com.demandware.carbonj.service.accumulator.MetricAggregationMethod;
import com.demandware.carbonj.service.accumulator.MetricAggregationPolicy;
import com.demandware.carbonj.service.accumulator.MetricAggregationPolicyProvider;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRecoveryAccumulator extends BaseTest {

    private AtomicInteger flushedPoints;
    private AtomicInteger latePoints;

    @BeforeEach
    public void setUp() {
        flushedPoints = new AtomicInteger(0);
        latePoints = new AtomicInteger(0);
    }

    @Test
    public void testAllRecoveryPoints() {
        MetricAggregationPolicyProvider mock = getMetricAggregationPolicyProvider();
        RecoveryAccumulator accumulator = new RecoveryAccumulator( metricRegistry,"test", mock, 1000,  2,
                new DefaultSlotStrategy(), new CountingLatePointLogger(latePoints), mock(NamespaceCounter.class));

        addAndVerify(accumulator, 0, toMillis(900),1, 1, 0);

        addAndVerify(accumulator, 60, toMillis(901),2, 2, 0);

        // there will be no flush here as there should be at least 15 secs pause between flushes.
        addAndVerify(accumulator, 180, toMillis(902),3, 3, 0);

        // now, there will be a flush since 20 seconds have elapsed.
        addAndVerify(accumulator, 180, toMillis(920),3, 2, 1);

        // another 1 second elapsed.
        addAndVerify(accumulator, 240, toMillis(921),3, 3, 1);

        // ignore late points.
        addAndVerify(accumulator, 0, toMillis(922),3, 3, 1);

        // ignore late point but, flush
        addAndVerify(accumulator, 60, toMillis(936),3, 2, 2);

        // recovery data has ended.   But,  there have been few slots that have not been flushed yet.   These slots
        // should be flushed forcefully.

        accumulator.rollUp(this::incrementFlushPoints, toMillis(946), false);
        assertEquals(2, flushedPoints.get());

        accumulator.rollUp(this::incrementFlushPoints, toMillis(1100), true);
        assertEquals(4, flushedPoints.get());

        assertEquals(0, accumulator.getTimeSlots().size());

        assertEquals(4, latePoints.get());

        assertEquals(240, accumulator.getMaxClosedSlotTs());
        assertInstanceOf(DefaultSlotStrategy.class, accumulator.getSlotStrategy());
        accumulator.refreshStats();
        accumulator.dumpStats();
        accumulator.reset();
        assertEquals(0, accumulator.getMaxClosedSlotTs());
    }

    private long toMillis(int sec) {
        return sec * 1000L;
    }

    @SuppressWarnings("unused")
    private void incrementFlushPoints(DataPoints dataPoints) {
        flushedPoints.incrementAndGet();
    }

    private void addAndVerify(RecoveryAccumulator accumulator, int timeInSecs, long currentTimeInMillis, int expectedSlotsBeforeFlush,
                              int expectedSlotsAfterFlush, int expectedTotalFlushedSoFar) {
        accumulator.add(new DataPoint("metric1", 1, timeInSecs));
        accumulator.add(new DataPoint("metric2", 1, timeInSecs));
        assertEquals(expectedSlotsBeforeFlush, accumulator.getTimeSlots().size());
        accumulator.rollUp(this::incrementFlushPoints, currentTimeInMillis, false);
        assertEquals(expectedTotalFlushedSoFar, flushedPoints.get());
        assertEquals(expectedSlotsAfterFlush, accumulator.getTimeSlots().size());
        // Assert.assertEquals(expectedMaxClosedTs, accumulator.getMaxClosedSlotTs());
    }

    private MetricAggregationPolicyProvider getMetricAggregationPolicyProvider() {
        MetricAggregate sum = new MetricAggregate("sum", MetricAggregationMethod.SUM, false);
        MetricAggregate avg = new MetricAggregate("avg", MetricAggregationMethod.AVG, false);
        MetricAggregationPolicy map = new MetricAggregationPolicy(1, Arrays.asList(sum, avg));
        MetricAggregationPolicyProvider mock = mock(MetricAggregationPolicyProvider.class);
        when(mock.metricAggregationPolicyFor(any(String.class))).thenReturn(map);
        return mock;
    }

    @Test
    public void testLatePoints() {
        MetricAggregationPolicyProvider mock = getMetricAggregationPolicyProvider();
        RecoveryAccumulator accumulator = new RecoveryAccumulator(metricRegistry,"test1", mock, 1000,  2,
                new DefaultSlotStrategy(), new CountingLatePointLogger(latePoints), mock(NamespaceCounter.class));
        accumulator.add(getDataPoint(20));
        accumulator.add(getDataPoint(40));
        accumulator.add(getDataPoint(120));
        accumulator.add(getDataPoint(180));
        assertEquals(0, latePoints.get());
        accumulator.add(getDataPoint(20));
        assertEquals(1, latePoints.get());
        accumulator.add(getDataPoint(130));
        assertEquals(1, latePoints.get());
    }

    private DataPoint getDataPoint(int ts) {
        return new DataPoint("metric", 1, ts);
    }
}
