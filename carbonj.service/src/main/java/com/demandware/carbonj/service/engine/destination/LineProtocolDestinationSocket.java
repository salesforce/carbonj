/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineProtocolDestinationSocket
    extends Destination
    implements LineProtocolDestination
{
    private static Logger log = LoggerFactory.getLogger( LineProtocolDestinationSocket.class );

    private final MetricRegistry metricRegistry;
    final String ip;

    volatile boolean stop = false;

    final int port;

    final ArrayBlockingQueue<DataPoint> q;

    final int batchSize;


    String queueSizeGaugeName()
    {
        return MetricRegistry.name( name, "queue" );
    }

    public LineProtocolDestinationSocket( MetricRegistry metricRegistry, String type, String ip, int port, int queueSize, int batchSize )
    {
        super(metricRegistry, "dest." + type + "." + StringUtils.replace( ip, ".", "_" ) + "_" + port);
        this.metricRegistry = metricRegistry;
        this.ip = Preconditions.checkNotNull( ip );
        this.port = port;
        this.batchSize = batchSize;
        q = new ArrayBlockingQueue<>( queueSize );
        this.setDaemon( true );
        this.start();
        log.info( "Started dest " + this );
    }

    @Override
    public String toString()
    {
        return ip + ":" + port;
    }

    @Override
    public void accept( DataPoint t )
    {
        received.mark();
        if ( stop || q.offer( t ) )
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
            long timeout = 30000;
            this.join(timeout);
            // check if the thread is still alive
            if(this.isAlive())
            {
                log.warn(String.format("destination %s hasn't stopped after %s seconds. Trying to interrupt...",
                        this, timeout));
                this.interrupt();
                this.join(10000);
                if( this.isAlive() )
                {
                    log.error(String.format(
                            "destination %s failed to stop after explicit interrupt(). Consider it closed (Abandoning this destination).",
                            this));
                }
                else
                {
                    log.warn("Dest " + this + " stopped after explicit interrupt().");
                }
            }
            else {
                log.info("Dest " + this + " stopped.");
            }
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
        Socket sock = null;
        String name = queueSizeGaugeName();
        try
        {
            metricRegistry.remove(name);  // remove if it already exists.
            metricRegistry.register(name, new Gauge<Number>()
            {
                @Override
                public Number getValue()
                {
                    return q.size();
                }
            } );

            List<DataPoint> buf = new ArrayList<>( batchSize );
            while ( true )
            {
                if ( stop )
                {
                    drain(pw);
                    return;
                }
                try
                {
                    if ( null == pw )
                    {
                        log.info( "Connecting " + this );
                        sock = new Socket( ip, port );
                        log.info( "Connected " + this );
                        pw = new PrintWriter( sock.getOutputStream() );
                    }

                    if( q.drainTo( buf, batchSize ) == 0 )
                    {
                        DataPoint p = q.poll( 100, TimeUnit.MILLISECONDS );
                        if( p == null )
                        {
                            Thread.yield();
                            continue;
                        }
                        else
                        {
                            buf.add( p );
                        }
                    }

                    for(DataPoint p : buf)
                    {
                        pw.println(p);
                    }
                    // also flushes
                    if ( pw.checkError() )
                    {
                        throw new IOException( "printwriter errored on write" );
                    }
                    sent.mark( buf.size() );

                }
                catch ( Exception e )
                {
                    log.error( "Failure sending metrics. Will try to reconnect. " + this, e );
                    try
                    {
                        Closeables.close( pw, true );
                    }
                    catch ( IOException e2 )
                    {
                    }
                    pw = null;
                    try
                    {
                        Closeables.close( sock, true );
                    }
                    catch ( IOException e2 )
                    {
                    }
                    sock = null;
                    try
                    {
                        Thread.sleep( 1000 ); // try reconnect in a sec
                    }
                    catch ( InterruptedException e1 )
                    {
                        throw Throwables.propagate( e );
                    }
                }
                finally
                {
                    buf.clear();
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
                Closeables.close( sock, true );
            }
            catch ( IOException e )
            {
            }
            metricRegistry.remove(name);
            log.info("Exited " + name);
        }
    }

    private void drain(PrintWriter pw) {
        assert stop;  // should be drained only when stop is requested.

        if (pw == null) {
            return;
        }

        try {
            int size = q.size();
            for(DataPoint p : q) {
                pw.println(p);
            }
            sent.mark( size );
            log.info(String.format("Drained %d points!", size));

            pw.flush();
        } catch (Throwable e) {
            ;  //ignore
        }
    }
}
