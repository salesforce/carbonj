/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.google.common.base.Preconditions;

public class InputQueue
    extends Thread
    implements Consumer<DataPoint>, StatsAware
{
    private enum RejectPolicy
    {
        drop, block
    }

    private static Logger log = LoggerFactory.getLogger( InputQueue.class );

    private final Meter rejects;

    private final Timer blocks;

    // sample queue depth every 10 sec.
    private final Histogram queueDepthHistogram;

    private final Gauge<Number> queueDepth;

    private final Meter received;

    private final Meter taskCount;

    private final Meter emptyQueueReadCount;

    private final Histogram pointsPerTask;

    private final PointProcessor pointProcessor;

    private int batchSize;

    private ArrayBlockingQueue<DataPoint> queue;

    private volatile boolean stop = false;

    private long emptyQueuePauseMillis = 100;

    private RejectionHandler rh;

    private final int queueCapacity;

    public InputQueue(MetricRegistry metricRegistry, String name, PointProcessor pointProcessor, int queueSize,
                      String rejectPolicy, int batchSize,
                      long emptyQueuePauseMillis)
    {
        super( name );
        this.pointProcessor = Preconditions.checkNotNull(pointProcessor);
        this.batchSize = batchSize;
        this.emptyQueuePauseMillis = emptyQueuePauseMillis;
        this.queueCapacity = queueSize;
        this.queue = new ArrayBlockingQueue<>( queueSize );
        this.rh = queueRejectionHandler( rejectPolicy );

        this.rejects = metricRegistry.meter(
                MetricRegistry.name( "aggregator", "rejects" ) );
        this.blocks = metricRegistry.timer( MetricRegistry.name( "aggregator", "blocks" ) );
        this.queueDepthHistogram = metricRegistry.histogram( "queueDepthHist" );
        this.queueDepth = metricRegistry.register( queueSizeGaugeName(),
                (Gauge<Number>) ( ) -> queueDepthHistogram.getSnapshot().getMean() );
        this.received = metricRegistry.meter(
            MetricRegistry.name( "inputQueue", "received" ) );
        this.taskCount = metricRegistry.meter(
                MetricRegistry.name( "inputQueue", "taskCount" ) );
        this.emptyQueueReadCount = metricRegistry.meter(
                MetricRegistry.name( "inputQueue", "emptyQueueReadCount" ) );
        this.pointsPerTask = metricRegistry.histogram(
                MetricRegistry.name( "inputQueue", "pointsPerTask" ) );
    }

    public void drain()
    {
        DrainUtils.drain( queue );
        pointProcessor.drain();
    }

    @Override
    public void run()
    {

        // queue consumer loop.
        while ( true )
        {
            try
            {
                if ( stop )
                {
                    return;
                }

                if ( queue == null )
                {
                    stop = true;
                    return;
                }

                ArrayList<DataPoint> batch = new ArrayList<>( batchSize );

                if ( queue.drainTo( batch, batchSize ) == 0 )
                {
                    DataPoint point = queue.poll( emptyQueuePauseMillis, TimeUnit.MILLISECONDS );
                    if ( null == point )
                    {
                        emptyQueueReadCount.mark();
                        continue;
                    }
                    batch.add( point );
                }

                taskCount.mark();
                pointsPerTask.update( batch.size() );
                try
                {
                    pointProcessor.process(batch);
                }
                catch ( Throwable t )
                {
                    log.error( "Error when processing batch of points", t );
                }
            }
            catch ( Throwable e )
            {
                log.error( "Failure processing points queue.", e );
            }
        }
    }



    @PostConstruct
    @Override
    public synchronized void start()
    {
        super.start();
        log.info( "started " + this );
    }

    @PreDestroy
    void close()
    {
        log.info( "Stopping " + this );

        stop = true;
        pointProcessor.close();
        log.info( this + " stopped." );
    }

    /**
     * Rejection handler for placing data points on input queue.
     *
     * @param rejectPolicy
     * @return
     */
    private RejectionHandler<DataPoint> queueRejectionHandler( String rejectPolicy )
    {
        RejectPolicy policy = RejectPolicy.valueOf( rejectPolicy );
        switch ( policy )
        {
            default:
                throw new RuntimeException( String.format( "Invalid rejectPolicy value [%s].", rejectPolicy ) );
            case block:
                log.info("Metric blocked - input queue");
                return ( q, t ) -> {
                    refreshStats(); // good time to refresh stats
                    try (Context c = blocks.time())
                    {
                        q.put( t );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( "Unexpected InterruptedException", e );
                    }
                };
            case drop:
                log.info("Metric dropped - input queue");
                return ( q, t ) -> {
                    refreshStats(); // good time to refresh stats
                    rejects.mark();
                    if ( log.isDebugEnabled() )
                    {
                        log.debug( "Metric rejected. Queue size " + q.size() + ". Total rejects " + rejects.getCount() );
                    }
                    t.drop();
                };
        }
    }

    @Override
    public void accept( final DataPoint t )
    {
        received.mark();
        if ( queue == null )
        {
            pointProcessor.process( Collections.singletonList( t ) );
        }
        else
        {
            if ( queue.offer( t ) )
            {
                return;
            }
            rh.rejected( queue, t );
        }
    }


    @Override
    public void dumpStats()
    {
        pointProcessor.dumpStats();
        log.info( String.format(
            "stats: rejects=%s queueSize=%s",
            rejects.getCount(), queueDepth.getValue()) );
    }

    public void refreshStats()
    {
        pointProcessor.refreshStats();
        queueDepthHistogram.update( queuedPointsCount() );
    }

    public int queuedPointsCount()
    {
        if ( queue == null )
        {
            return 0;
        }
        else
        {
            return queue.size();
        }
    }

    public int queueCapacity()
    {
        return queueCapacity;
    }

    private String queueSizeGaugeName()
    {
        return MetricRegistry.name( "aggregator", "queueSize" );
    }

}
