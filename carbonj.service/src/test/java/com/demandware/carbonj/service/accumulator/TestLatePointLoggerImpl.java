/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLatePointLoggerImpl {
    @Test
    public void test() {
        MetricRegistry metricRegistry = new MetricRegistry();
        LatePointLoggerImpl latePointLogger = new LatePointLoggerImpl(metricRegistry);
        latePointLogger.logLatePoint(new DataPoint("foo.bar", 123, 60), 120, LatePointLogger.Reason.SLOT_CLOSED, "Context");
        assertEquals(1, metricRegistry.getCounters().get("aggregator.skippedDelayed").getCount());
        assertEquals(60, metricRegistry.getHistograms().get("aggregator.pointAgeHistogram").getSnapshot().get95thPercentile());
    }
}
