/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.core.config;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.core.metric.Util;
import com.sfcc.um.metrics_reporter.reporter.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * Configures the metric sub-system.
 */
@Configuration public class cfgMetric
{
    private static final Logger LOG = LoggerFactory.getLogger( cfgMetric.class );

    private MetricRegistry metricRegistry;

    private GraphiteReporter graphiteReporter;

    //graphite.host=relay-udp.carbonj.svc.cluster.local
    //graphite.port=2003
    //graphite.transport=UDP
    // CBAYER: I haven't found any evidence that this setting is honored by codahale/dropwizard lib 4.0 that we use. Neither
    // is it referenced in our code. Will not support this value then.
    //graphite.transport.tcp.reconnect=true

    @Value( "${graphite.host:localhost}" ) private String graphiteHost;

    @Value( "${graphite.port:2003}" ) private int graphitePort;

    @Value( "${graphite.transport:UDP}" ) private String graphiteTransport;

    @Value( "${graphite.prefix:}" ) private String graphitePrefix;

    @Value( "${dw.podId:-1}" ) private int podId;

    @Value( "${dw.groupId:}" ) private String groupId;

    @Value( "${dw.svc.version:}" ) private String svcVersion;

    @Bean public MetricRegistry metricRegistry()
    {
        // expose the singleton, static CodaHale metric registry
        this.metricRegistry = new MetricRegistry();
        return metricRegistry;
    }

    @Bean public DatabaseMetrics databaseMetrics(MetricRegistry metricRegistry )
    {
        return new DatabaseMetrics( metricRegistry );
    }

    @Bean public GraphiteReporter graphiteReporter( MetricRegistry metricRegistry )
    {
        try
        {
            GraphiteReporter graphiteReporter =
                            Util.getGraphiteReporter( metricRegistry, graphiteHost, graphitePort, graphiteTransport,
                                            Util.getGraphiteMetricPrefix( graphitePrefix, podId, groupId, svcVersion ) );
            if ( graphiteReporter != null )
            {
                graphiteReporter.start( 60, TimeUnit.SECONDS );
            }
            else
            {
                LOG.error( "graphite reporter is null" );
            }
            this.graphiteReporter = graphiteReporter;
        }
        catch ( Exception e )
        {
            LOG.error( "Error starting graphite reporter" + e );
        }
        return graphiteReporter;
    }

    @PreDestroy public void preDestroy()
    {
        // upon shutdown (of the context) remove all metrics
        // this is primarily useful for test cases to cleanup the (static) metric registry
        LOG.info( "Clearing metric registry" );
        metricRegistry.removeMatching( MetricFilter.ALL );
        if ( graphiteReporter != null )
        {
            graphiteReporter.stop();
        }
    }
}