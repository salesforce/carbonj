/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.regex.Pattern;

import static com.codahale.metrics.MetricRegistry.name;

public abstract class AbstractGraphiteTransport implements GraphiteTransport
{
    private static final Pattern WHITESPACE = Pattern.compile( "[\\s]+" );

    protected final MetricRegistry metricRegistry;

    AbstractGraphiteTransport(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;
    }

    public Meter failureCount()
    {
        return metricRegistry.meter( name("graphite", "transport", "error-counts") );
    }

    public Meter metricsCount()
    {
        return metricRegistry.meter( name( "graphite", "transport", "metric-counts") );
    }

    public Histogram batchSizes()
    {
        return metricRegistry.histogram( name( "graphite", "transport", "batch-sizes" ) );
    }

    protected String sanitize( String s )
    {
        return WHITESPACE.matcher( s ).replaceAll( "-" );
    }
}
