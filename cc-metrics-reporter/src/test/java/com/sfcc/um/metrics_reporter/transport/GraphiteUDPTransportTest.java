/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphiteUDPTransportTest {

    @Test
    public void testSendFailure() throws IOException {
        MetricRegistry metricRegistry = new MetricRegistry();
        GraphiteUDPTransport transport = new GraphiteUDPTransport("xyz.salesforce.com", 8080, 1, metricRegistry);
        transport.open();
        transport.send("key", "1", System.currentTimeMillis());
        transport.close();
        assertEquals(1L, metricRegistry.meter(name("graphite", "transport", "error-counts")).getCount());
    }

    @Test
    public void testSendSuccess() throws IOException {
        MetricRegistry metricRegistry = new MetricRegistry();
        GraphiteUDPTransport transport = new GraphiteUDPTransport("salesforce.com", 80, 1, metricRegistry);
        transport.open();
        transport.send("key", "1", System.currentTimeMillis());
        transport.close();
        assertEquals(0L, metricRegistry.meter(name("graphite", "transport", "error-counts")).getCount());
        assertEquals(1L, metricRegistry.meter(name("graphite", "transport", "metric-counts")).getCount());
    }
}
