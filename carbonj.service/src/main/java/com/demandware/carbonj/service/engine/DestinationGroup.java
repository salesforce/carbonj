/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import static org.apache.commons.lang3.StringUtils.split;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.destination.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import com.google.common.base.Throwables;

/**
 * Partitions data point stream across destinations defined within group. In other words, each
 * data point consumed by destination group will be sent to only one of the destinations within the group.
 *
 * If you want to send the same data point to multiple destinatins (i.e. replicate) you need to define multiple
 * destination groups.
 *
 * All destinations within a group reconfigured as one unit because addition or removal of destinations affects
 * logic that selects destination to use for data point.
 *
 */
class DestinationGroup
    implements Consumer<DataPoint>
{

    private static final Logger log = LoggerFactory.getLogger( DestinationGroup.class );

    private final MetricRegistry metricRegistry;

    private String dest;

    private LineProtocolDestination[] destinations;
    private int noOfDestinations;

    private String kinesisRelayRegion;

    DestinationGroup( MetricRegistry metricRegistry, String type, String dest, int queueSize, int batchSize, int refreshIntervalInMillis,
                      String destConfigDir, int maxWaitTimeInSecs, String kinesisRelayRegion, Boolean kinesisRelayRbacEnabled, String kinesisRelayAccount, String kinesisRelayRole)
    {
        this.metricRegistry = metricRegistry;
        this.dest = dest;

        List<LineProtocolDestination> destinationsList = new ArrayList<>();
        if ( StringUtils.isEmpty( dest ) )
        {
            return;
        }

        try
        {
            for ( String s : split( dest, ',' ) )
            {
                LineProtocolDestination d;
                s = s.trim();
                if( s.startsWith("null:") )
                {
                    d = new NullDestination(metricRegistry, type, "null");
                }
                else if( s.startsWith( "file:" ))
                {
                    String fileName = s.substring( "file:".length() );
                    d = new FileDestination(metricRegistry, type, fileName, queueSize );
                }

                else if( s.startsWith( "kinesis:" ))
                {
                    String kinesisStreamName = s.substring(("kinesis:".length()));
                    int kinesisBatchSize = 60000;
                    int threadCount = 5;
                    String destCfgFile = destConfigDir + File.separatorChar + "kinesis-"+kinesisStreamName+"-dest.conf";
                    InputStream input;
                    Properties destCfg;

                    try {
                        input = new FileInputStream( destCfgFile );
                        log.info(" Loading values from " + destCfgFile);
                        destCfg = new Properties();
                        destCfg.load(input);
                        kinesisBatchSize = Integer.parseInt(destCfg.getProperty("kinesisBatchSize", "60000"));
                        threadCount = Integer.parseInt(destCfg.getProperty("threadCount", "5"));
                    }
                    catch(FileNotFoundException e) {
                        log.warn(  destCfgFile + " not found in the classpath ");
                        log.info(" Falling back to default values ");
                    }

                    d = new KinesisDestination(metricRegistry, type, queueSize, kinesisStreamName, kinesisBatchSize, threadCount, maxWaitTimeInSecs, kinesisRelayRegion, kinesisRelayRbacEnabled, kinesisRelayAccount, kinesisRelayRole);

                }
                else if (s.startsWith("service:")) {
                    String nameAndPort =  s.substring(("service:".length()));
                    String[] addr = split( nameAndPort, ":" );
                    d = new LineProtocolDestinationsService(metricRegistry, type, addr[0], Integer.parseInt( addr[1] ), queueSize, batchSize, refreshIntervalInMillis);
                }
                else
                {
                    String[] addr = split( s, ":" );
                    d = new LineProtocolDestinationSocket( metricRegistry, type, addr[0], Integer.parseInt( addr[1] ), queueSize, batchSize );
                }
                destinationsList.add(d);
            }

            destinations = destinationsList.toArray(new LineProtocolDestination[destinationsList.size()]);
            noOfDestinations = destinations.length;

            if (noOfDestinations == 0) {
                throw new IllegalStateException("No of destinations is zero!");
            }
        }
        catch ( Exception e )
        {
            close( destinationsList );
            throw Throwables.propagate( e );
        }
    }

    DestinationGroup(MetricRegistry metricRegistry, LineProtocolDestination[] destinations) {
        if (destinations == null || destinations.length == 0) {
            throw new IllegalStateException();
        }

        this.metricRegistry = metricRegistry;
        this.destinations = destinations;
        noOfDestinations = destinations.length;
    }

    String getDest()
    {
        return dest;
    }

    void close()
    {
        close( Arrays.asList(destinations) );
    }

    private static void close( List<LineProtocolDestination> tt )
    {
        tt.forEach( LineProtocolDestination::closeQuietly );
    }

    @Override
    public void accept( DataPoint t )
    {
        try {
            if (noOfDestinations == 1) {
                destinations[0].accept(t);
            } else {
                Consumer<DataPoint> target = destinations[Math.abs(t.name.hashCode()) % noOfDestinations];
                target.accept(t);
            }
        } catch (Throwable e) {
            log.error("Exception while sending data points to a destination", e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !( o instanceof DestinationGroup ) )
            return false;

        DestinationGroup that = (DestinationGroup) o;

        return dest.equals( that.dest );

    }

    @Override
    public int hashCode()
    {
        return dest.hashCode();
    }

}
