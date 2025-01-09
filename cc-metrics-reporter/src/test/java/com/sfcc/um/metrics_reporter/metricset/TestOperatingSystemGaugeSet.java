/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.metricset;

import com.codahale.metrics.Metric;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOperatingSystemGaugeSet {
    @Test
    public void test() {
        OperatingSystemGaugeSet osg = new OperatingSystemGaugeSet();
        Map<String, Metric> metrics = osg.getMetrics();
        assertEquals(9, metrics.size());
        assertTrue(metrics.containsKey("committedVirtualMemorySize"));
        assertTrue(metrics.containsKey("totalSwapSpaceSize"));
        assertTrue(metrics.containsKey("freeSwapSpaceSize"));
        assertTrue(metrics.containsKey("processCpuTime"));
        assertTrue(metrics.containsKey("freePhysicalMemorySize"));
        assertTrue(metrics.containsKey("totalPhysicalMemorySize"));
        assertTrue(metrics.containsKey("fd.usage"));
        assertTrue(metrics.containsKey("systemCpuLoad"));
        assertTrue(metrics.containsKey("processCpuLoad"));
    }
}
