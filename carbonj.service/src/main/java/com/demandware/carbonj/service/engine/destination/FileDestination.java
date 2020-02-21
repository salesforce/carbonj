/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Destination that writes data points to a file.
 */
public class FileDestination
    extends Destination
    implements LineProtocolDestination
{
    private static Logger log = LoggerFactory.getLogger( FileDestination.class );

    final String filename;

    volatile boolean stop = false;

    final ArrayBlockingQueue<DataPoint> q;

    public FileDestination( MetricRegistry metricRegistry, String type, String filename, int queueSize )
    {
        super(metricRegistry, "dest." + type + "." + filename.replaceAll( "[\\. /]", "_" ));
        this.filename = Preconditions.checkNotNull( filename );
        q = new ArrayBlockingQueue<>( queueSize );
        this.setDaemon( true );
        this.start();
        log.info( "Started dest " + this );
    }

    @Override
    public String toString()
    {
        return filename;
    }

    @Override
    public void accept( DataPoint t )
    {
        if ( q.offer( t ) )
        {
            return;
        }
        drop.mark();
        if ( log.isDebugEnabled() )
        {
            log.debug( "Dropped->" + this + ". Queue size " + q.size() + ". Total dropped " + drop.getCount() );
        }
    }

    @Override
    public void closeQuietly()
    {
        try
        {
            close();
        }
        catch ( Exception e )
        {
            log.error( "Failed to close destination [" + this.toString() + "]", e );
        }
    }

    void close()
    {
        log.info( "Stopping dest " + this );
        stop = true;
        try
        {
            this.join();
            log.info( "Dest " + this + " stopped." );
        }
        catch ( InterruptedException e )
        {
            throw Throwables.propagate( e );
        }
    }

    @Override
    public void run()
    {
        PrintWriter pw = null;
        DataPoint m = null;
        try
        {
            registerQueueDepthGauge();

            while ( true )
            {
                if ( stop )
                {
                    return;
                }
                try
                {
                    if ( null == pw )
                    {
                        log.info( "Opening " + this );
                        File f = new File( filename );
                        pw = new PrintWriter( f );
                        log.info( "Created file >> " + f + " " + this );
                    }

                    if ( null == m )
                    {
                        m = q.poll( 100, TimeUnit.MILLISECONDS ); // give chance to check for "stop"
                        if ( null == m )
                        {
                            continue;
                        }
                    }
                    pw.println( m );
                    sent.mark();
                    if ( log.isDebugEnabled() )
                    {
                        log.debug( this + ":" + sent.getCount() + "<-" + m );
                    }
                    m = null;
                }
                catch ( Exception e )
                {
                    log.error( "Failure sending metrics. Will try later. " + this, e );
                    try
                    {
                        Closeables.close( pw, true );
                    }
                    catch ( IOException e2 )
                    {
                    }
                    try
                    {
                        Thread.sleep( 1000 ); // try reconnect in a sec
                    }
                    catch ( InterruptedException e1 )
                    {
                        throw Throwables.propagate( e );
                    }
                }
            }
        }
        catch ( Throwable e )
        {
            log.error( "Unhandled error. Exit protocol destination thread. " + this, e );
        }
        finally
        {
            try
            {
                Closeables.close( pw, true );
            }
            catch ( IOException e )
            {
            }
            unregisterQueueDepthGauge();
        }
    }

    String queueSizeGaugeName()
    {
        return MetricRegistry.name( name, "queue" );
    }

    private void unregisterQueueDepthGauge()
    {
        metricRegistry.remove( queueSizeGaugeName() );
    }

    private void registerQueueDepthGauge()
    {
        metricRegistry.register( queueSizeGaugeName(), new Gauge<Number>()
        {
            @Override
            public Number getValue()
            {
                return q.size();
            }
        } );
    }
}
