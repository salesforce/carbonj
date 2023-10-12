/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.strings.StringsCache;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class RelayRulesTest {

    @Before
    public void before() {
        new StringsCache(new MetricRegistry(), 5000000, 10000000, 180, 8);
        StringsCache.get("foo.bar");
        StringsCache.get("pod240.ecom.blade.jvm.gc.host.memory.heap.usage");
    }

    @Test
    public void testRelayRules() {
        RelayRules relayRules = new RelayRules("relay",
                new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("relay-rules.conf")).getFile()),
                "file", true, null);
        String[] destinationGroups = relayRules.getDestinationGroups("foo.bar");
        assertEquals(0, destinationGroups.length);
        destinationGroups = Objects.requireNonNull(StringsCache.getState("foo.bar")).getRelayDestinations();
        assertEquals(0, destinationGroups.length);
        destinationGroups = relayRules.getDestinationGroups("pod240.ecom.host.jvm.memory.heap.usage");
        assertEquals(2, destinationGroups.length);
        assertEquals("kinesis:umon-prd-v2-cjajna", destinationGroups[0]);
        assertEquals("kinesis:umon-prd-v2-cjArgus", destinationGroups[1]);
        destinationGroups = Objects.requireNonNull(StringsCache.getState("pod240.ecom.host.jvm.memory.heap.usage")).getRelayDestinations();
        assertEquals(2, destinationGroups.length);
        assertEquals("kinesis:umon-prd-v2-cjajna", destinationGroups[0]);
        assertEquals("kinesis:umon-prd-v2-cjArgus", destinationGroups[1]);
    }
}
