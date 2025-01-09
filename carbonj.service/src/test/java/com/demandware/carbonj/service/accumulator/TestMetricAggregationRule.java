/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMetricAggregationRule {
    @Test
    public void test() {
        String aggregationRule = "ocapi.<realm>.<tenant>.<metric> (60) c = custom1 pod[0-9]{3,6}.ecom.<realm>.<tenant>.*.*.ocapi.clients.*.<<metric>>";
        MetricAggregationRule metricAggregationRule = MetricAggregationRule.parseDefinition(aggregationRule, 1, false);
        assertFalse(metricAggregationRule.isStopRule());
        assertEquals(MetricAggregationMethod.CUSTOM1, metricAggregationRule.getMethod());
        assertTrue(metricAggregationRule.equals(metricAggregationRule));
        assertFalse(metricAggregationRule.equals(new Object()));
        assertTrue(metricAggregationRule.hashCode() > 0);
        assertEquals(String.format("com.demandware.carbonj.service.accumulator.MetricAggregationRule@%x", System.identityHashCode(metricAggregationRule)), metricAggregationRule.toString());
        assertFalse(metricAggregationRule.equals(MetricAggregationRule.parseDefinition(
                "ocapi.<realm>.<tenant>.<metric> (60) c = sum pod[0-9]{3,6}.ecom.<realm>.<tenant>.*.*.ocapi.clients.*.<<metric>>", 1, false)));
        MetricAggregationRule.Result result = metricAggregationRule.apply("pod807.ecom.bgzz.bgzz_prd.blade_1.bgzz_prd.ocapi.clients.client.foo.bar");
        assertTrue(result.equals(result));
        assertFalse(result.equals(null));
        assertNotEquals(0, result.hashCode());
        assertEquals("Result{aggregateName=ocapi.bgzz.bgzz_prd.foo.bar, method=CUSTOM1, dropOriginal=false}", result.toString());
        assertFalse(result.equals(metricAggregationRule.apply("pod807.ecom.bgzz.bgzz_prd.blade_1.bgzz_prd.ocapi.clients.client.foo.bar2")));

        String aggregationRule2 = "ocapi.<realm>.<tenant>.<metric> (60) cc = custom1 pod[0-9]{3,6}.ecom.<realm>.<tenant>.*.*.ocapi.clients.*.<<metric>>";
        try {
            MetricAggregationRule.parseDefinition(aggregationRule2, 2, false);
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            assertEquals("Unsupported flag: [cc]", e.getMessage());
        }
    }
}
