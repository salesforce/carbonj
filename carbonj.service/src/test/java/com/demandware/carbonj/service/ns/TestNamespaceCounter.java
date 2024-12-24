/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.ns;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNamespaceCounter {
    @Test
    public void testDefault() {
        MetricRegistry metricRegistry  = new MetricRegistry();
        NamespaceCounter namespaceCounter = new NamespaceCounter(metricRegistry, 0);
        namespaceCounter.count("foo.bar");
        assertTrue(namespaceCounter.exists("foo.bar"));
        assertTrue(namespaceCounter.getLiveNamespaces().contains("foo"));
        namespaceCounter.removeInactive();
        assertFalse(namespaceCounter.exists("foo.bar"));
    }
}
