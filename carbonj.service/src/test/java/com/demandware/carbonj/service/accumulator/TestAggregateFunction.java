/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestAggregateFunction {
    @Test
    public void testAggregateFunction() {
        AggregateFunction aggregateFunction = AggregateFunction.create("foo.bar.p95", MetricAggregationMethod.CUSTOM1);
        assertInstanceOf(AggregateFunction.AvgAggregateFunction.class, aggregateFunction);
        assertEquals(0, aggregateFunction.apply());
        try {
            aggregateFunction.getValues();
            fail("Should have thrown an exception");
        } catch (UnsupportedOperationException e) {
        }
        aggregateFunction = AggregateFunction.create("foo.bar.latency", MetricAggregationMethod.LATENCY);
        assertInstanceOf(AggregateFunction.LatencyAggregateFunction.class, aggregateFunction);
        assertEquals(0, aggregateFunction.apply());
    }
}
