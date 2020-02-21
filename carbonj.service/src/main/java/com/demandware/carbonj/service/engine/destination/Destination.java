/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

abstract public class Destination extends Thread
{
    Meter sent;
    Meter received;
    Meter drop;
    MetricRegistry metricRegistry;
    protected String name;

    public Destination(MetricRegistry metricRegistry, String name)
    {
        this.name = name;
        received = metricRegistry.meter( MetricRegistry.name( name, "recv" ) );
        drop = metricRegistry.meter( MetricRegistry.name( name, "drop" ) );
        sent = metricRegistry.meter( MetricRegistry.name( name, "sent" ) );
        this.metricRegistry = metricRegistry;
    }
}
