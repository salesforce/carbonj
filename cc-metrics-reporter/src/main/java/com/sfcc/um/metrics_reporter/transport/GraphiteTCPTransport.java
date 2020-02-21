/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.Socket;

public class GraphiteTCPTransport extends AbstractGraphiteTransport
{
    private final String host;

    private final int port;

    private Socket socket;

    private StringWriter writer;

    private OutputStream out;

    private final int batchSize;

    private int metricCounter;

    private Logger LOG = LogManager.getLogger( GraphiteTCPTransport.class );

    GraphiteTCPTransport( String host, int port, int batchSize, MetricRegistry metricRegistry )
    {
        super(metricRegistry);
        this.batchSize = batchSize;
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the server.
     *
     * @throws IOException if there is an error connecting
     */
    @Override
    public synchronized GraphiteTransport open()
                    throws IOException
    {
        if( socket != null || out != null )
        {
            throw new IllegalStateException("already connected.");
        }

        try
        {
            socket = new Socket(host, port);
            out = new BufferedOutputStream(socket.getOutputStream());
        }
        catch(Exception e)
        {
            closeConnection();
        }

        return this;
    }

    /**
     * Sends the given measurement to the server.
     *
     * This implementation follows closely the logic implemented in GraphiteUDPTransport to provide internal metrics
     * like batchSize, etc consistent with GraphiteUDPTransport.
     *
     *
     * @param name the name of the metric
     * @param value the value of the metric
     * @param timestamp the timestamp of the metric
     * @throws IOException if there was an error sending the metric
     */
    @Override
    public synchronized void send( String name, String value, long timestamp )
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
            send();
        }
    }

    private void send()
    {
        if ( writer != null )
        {
            try
            {
                writer.flush();

                LOG.debug( "GraphiteTCPTransport Sending: {}",
                                writer.toString().trim().replace( "\n", "" ) );

                byte[] payload = writer.toString().getBytes( Charsets.UTF_8 );
                if ( payload.length > 0 )
                {
                    out.write(payload);
                    out.flush();
                    // maintain some metrics to track traffic
                    batchSizes().update( payload.length );
                    metricsCount().mark( metricCounter );
                }
            }
            catch ( IOException e )
            {
                failureCount().mark();
                LOG.error( "Failed sending metrics to '{}:{}' ", host, port );
            }
            finally
            {
                writer = null;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        // send whatever is remaining of the last batch
        try
        {
            send();
        }
        finally
        {
            closeConnection();
        }
    }

    private void closeConnection()
    {
        try
        {
            out.close();
        }
        catch ( Exception e )
        {
            LOG.error( e.getMessage() );
        }

        try
        {
            socket.close();
        }
        catch ( Exception e )
        {
            LOG.error( e.getMessage() );
        }

        this.out = null;
        this.socket = null;
    }
}
