/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.MetricRegistry;

public class GraphiteTransportBuilder
{
    private final String host;

    private final int port;

    private int batchSize;

    private MetricRegistry metricRegistry;

    private final GraphiteTransportType transportType;

    public GraphiteTransportBuilder(GraphiteTransportType transportType,
                                    String host,
                                    int port,
                                    MetricRegistry metricRegistry)
    {
        this.transportType = transportType;
        this.host = host;
        this.port = port;
        this.metricRegistry = metricRegistry;
        this.batchSize = 25;
    }

    public GraphiteTransportBuilder batchSize(int batchSize )
    {
        this.batchSize = batchSize;
        return this;
    }

    public GraphiteTransportBuilder metricRegistry(MetricRegistry metricRegistry )
    {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public GraphiteTransport build()
    {
        GraphiteTransport t;
        switch( transportType )
        {
            case UDP:
                t = new GraphiteUDPTransport( host, port, batchSize, metricRegistry );
                break;
            case TCP:
                t = new GraphiteTCPTransport(host, port, batchSize, metricRegistry);
                break;
            default:
                throw new RuntimeException("Unsupported transport type: " + transportType);
        }
        return t;
    }
}
