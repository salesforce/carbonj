/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator.recovery;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.LatePointLogger;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRecoveryLatePointLogger {
    @Test
    public void testDefault() {
        MetricRegistry metricRegistry = new MetricRegistry();
        RecoveryLatePointLogger logger = new RecoveryLatePointLogger(metricRegistry);
        int current = (int) (System.currentTimeMillis()/ 1000);
        DataPoint dataPoint = new DataPoint("foo.bar", 123, current);
        logger.logLatePoint(dataPoint, current, LatePointLogger.Reason.SLOT_EXPIRED, "test");
        assertEquals(1, metricRegistry.getCounters().get("aggregator.recoveryDelayed").getCount());
    }
}
