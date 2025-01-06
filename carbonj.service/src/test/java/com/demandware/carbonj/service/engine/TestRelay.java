/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestRelay {
    @Test
    public void testRelay() {
        MetricRegistry metricRegistry = new MetricRegistry();
        File rulesFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("relay-rules.conf")).getFile());
        Relay relay = new Relay(metricRegistry, "relay", rulesFile, 1, 1, 60000, "/tmp", 60000,
                Region.US_EAST_1.id(), "file", true, null, false, "", "");
        assertFalse(relay.router.isEmpty());
        DataPoints dataPoints = new DataPoints(List.of(new DataPoint("foo.bar", 1, (int) (System.currentTimeMillis() / 1000))));
        relay.accept(dataPoints);
        assertEquals(1, relay.router.received.getCount());
    }
}
