/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class TestGraphiteTransportBuilder {
    @Test
    public void testBuild() {
        MetricRegistry metricRegistry = new MetricRegistry();
        GraphiteTransportBuilder graphiteTransportBuilder = new GraphiteTransportBuilder(GraphiteTransportType.TCP, "localhost", 80, metricRegistry);
        GraphiteTransport graphiteTransport = graphiteTransportBuilder.batchSize(1).metricRegistry(metricRegistry).build();
        assertInstanceOf(GraphiteTCPTransport.class, graphiteTransport);
        graphiteTransportBuilder = new GraphiteTransportBuilder(GraphiteTransportType.UDP, "localhost", 80, metricRegistry);
        graphiteTransport = graphiteTransportBuilder.build();
        assertInstanceOf(GraphiteUDPTransport.class, graphiteTransport);
    }
}
