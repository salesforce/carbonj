/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.base.MoreObjects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

public class GraphiteUDPTransport extends AbstractGraphiteTransport
{
    private InetSocketAddress address;

    private DatagramSocket socket;

    private StringWriter writer;

    private final int batchSize;

    private int metricCounter;

    private final String host;

    private final int port;

    private final Logger LOG = LogManager.getLogger( getClass() );

    GraphiteUDPTransport( String host, int port, int batchSize, MetricRegistry metricRegistry )
    {
        super(metricRegistry);
        this.batchSize = batchSize;
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the server.
     *
     * @throws IllegalStateException if the client is already connected
     * @throws IOException if there is an error connecting
     */
    @Override
    public GraphiteTransport open() throws IOException
    {
        // Support IP address changes ar runtime
        this.address = new InetSocketAddress( host, port );
        this.socket = new DatagramSocket( );
        return this;
    }

    /**
     * Sends the given measurement to the server.
     *
     * @param name the name of the metric
     * @param value the value of the metric
     * @param timestamp the timestamp of the metric
     * @throws IOException if there was an error sending the metric
     */
    @Override
    public void send( String name, String value, long timestamp )
                    throws IOException
    {
        // create the string writer with a size estimate to fit metrics for our batch size
        if ( writer == null )
        {
            writer = new StringWriter( batchSize * 80 );
            metricCounter = 0;
        }

        writer.append( sanitize( name ) );
        writer.append( ' ' );
        writer.append( sanitize( value ) );
        writer.append( ' ' );
        writer.append( Long.toString( timestamp ) );
        writer.append( '\n' );
        metricCounter++;

        if ( metricCounter >= batchSize )
        {
            sendPacket();
        }
    }

    private void sendPacket()
    {
        if ( writer != null )
        {
            try
            {
                writer.flush();
                byte[] payload = writer.toString().getBytes( Charsets.UTF_8 );
                if ( payload.length > 0 )
                {
                    socket.send( new DatagramPacket( payload, payload.length, address ) );

                    // maintain some metrics to track traffic
                    batchSizes().update( payload.length );
                    metricsCount().mark( metricCounter );
                }
            }
            catch ( Exception e )
            {
                failureCount().mark();
                LOG.error( "Failed sending UDP datagram to '{}'", address );
            }
            finally
            {
                writer = null;
            }
        }
    }

    @Override
    public void close()
    {
        try
        {
            // send whatever is remaining of the last batch
            sendPacket();
        }
        finally
        {
            try
            {
                socket.close();
            }
            finally
            {
                this.socket = null;
            }
        }
    }

    @Override
    public String toString()
    {
        if ( address != null )
        {
            return MoreObjects.toStringHelper( this ).add( "address", address ).toString();
        }
        else
        {
            return MoreObjects.toStringHelper( this ).add( "host", host ).add( "port", port ).toString();
        }
    }
}