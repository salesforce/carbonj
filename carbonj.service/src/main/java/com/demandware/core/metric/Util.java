/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.core.metric;

import com.codahale.metrics.MetricRegistry;
import com.sfcc.um.metrics_reporter.reporter.GraphiteReporter;
import com.sfcc.um.metrics_reporter.transport.GraphiteTransport;
import com.sfcc.um.metrics_reporter.transport.GraphiteTransportBuilder;
import com.sfcc.um.metrics_reporter.transport.GraphiteTransportType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Util
{
    private static final Logger LOG = LoggerFactory.getLogger( Util.class );

    private static GraphiteReporter graphiteReporter = null;

    public static String getGraphiteMetricPrefix( String graphiteMetricsPrefix, int podId, String groupId,
                                                  String version )
                    throws UnknownHostException
    {
        String fqdn = InetAddress.getLocalHost().getHostName();
        String hostname =  fqdn.substring(0, fqdn.indexOf( "." ) == -1 ? fqdn.length() : fqdn.indexOf( "." ) );
        // support legacy DW prefixes if podId and groupId are set:
        if ( !StringUtils.isEmpty( groupId ) && podId > 0 && !StringUtils.isEmpty( version ) )
        {
            graphiteMetricsPrefix = String.format( "pod%s.%s.%s.%s.%s", podId, groupId, "carbonj", hostname,
                            version.replaceAll( "\\.", "_" ));
        }
        else if ( StringUtils.isEmpty( graphiteMetricsPrefix ) )
        {
            graphiteMetricsPrefix = "um.dev.carbonj." + hostname;
        }
        return graphiteMetricsPrefix;
    }

    public static synchronized GraphiteReporter getGraphiteReporter( MetricRegistry metricRegistry, String graphiteHost,
                                                                     int graphitePort, String graphiteProtocol,
                                                                     String metricPrefix )
    {
        if ( graphiteReporter != null )
        {
            return graphiteReporter;
        }

        graphiteProtocol.toUpperCase();
        LOG.info( "Graphite Host: {}", graphiteHost );
        LOG.info( "Graphite Port: {}", graphitePort );
        LOG.info( "Graphite Protocol: {}", graphiteProtocol );
        LOG.info( "Graphite Metric Prefix: {}", metricPrefix );

        GraphiteTransportType transportType = GraphiteTransportType.valueOf( graphiteProtocol );
        GraphiteTransport transport =
                        new GraphiteTransportBuilder( transportType, graphiteHost, graphitePort, metricRegistry )
                                        .build();
        graphiteReporter = GraphiteReporter.forRegistry( metricRegistry ).withPrefix( metricPrefix ).build( transport );

        return graphiteReporter;
    }
}
