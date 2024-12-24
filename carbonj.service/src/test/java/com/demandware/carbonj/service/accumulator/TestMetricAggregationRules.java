/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMetricAggregationRules {
    @Test
    public void test() {
        MetricAggregationRules metricAggregationRules = new MetricAggregationRules(1, new ArrayList<>());
        assertTrue(metricAggregationRules.isEmpty());
        assertEquals("MetricAggregationRules{revision=1, rules=[]}", metricAggregationRules.toString());
    }
}
