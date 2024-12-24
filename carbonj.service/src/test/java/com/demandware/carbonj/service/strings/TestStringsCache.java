/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.strings;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestStringsCache {
    @Test
    public void testDefault() {
        MetricRegistry metricRegistry  = new MetricRegistry();
        new StringsCache(metricRegistry, 0, 0, 0, 0);
        assertNull(StringsCache.getState("foo.bar"));
        new StringsCache(metricRegistry, 1, 1, 1, 1);
        StringsCache.State state = StringsCache.getState("foo.bar");
        assertNotNull(state);
        assertEquals("foo.bar", state.getKey());
        assertNull(state.getBlackListed());
        assertNull(state.getRelayDestinations());
        assertTrue(state.getAggregationRuleMap().isEmpty());
    }
}
