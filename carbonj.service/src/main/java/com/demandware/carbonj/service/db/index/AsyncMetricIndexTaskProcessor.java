/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

@Deprecated
public class AsyncMetricIndexTaskProcessor
    extends Thread
{
    private static Logger log = LoggerFactory
        .getLogger( com.demandware.carbonj.service.db.index.AsyncMetricIndexTaskProcessor.class );

    private static Meter submitted;

    private static Meter rejected;

    private static Meter received;

    private static Meter emptyQueueReadCount;

    // sample queue depth every 10 sec.
    private static Histogram queueDepthHistogram;

    private static Gauge<Number> queueDepth;

    private ArrayBlockingQueue<Runnable> queue;

    private volatile boolean stop = false;

    private long emptyQueuePauseMillis = 100;

    private int batchSize = 100;

    private final MetricIndex index;

    public AsyncMetricIndexTaskProcessor( MetricRegistry metricRegistry, int queueSize, long emptyQueuePauseMillis, int batchSize, MetricIndex index )
    {
        super( AsyncMetricIndexTaskProcessor.class.getSimpleName() );
        log.info( String.format( "queueSize=%s, emptyQueuePauseMillis=%s, batchSize=%s", queueSize,
            emptyQueuePauseMillis, batchSize ) );
        this.emptyQueuePauseMillis = emptyQueuePauseMillis;
        this.queue = new ArrayBlockingQueue<>( queueSize );
        this.batchSize = batchSize;
        this.index = Preconditions.checkNotNull( index );


       submitted = metricRegistry.meter(
                MetricRegistry.name( "asyncMetricIndexTasks", "submitted" ) );

       rejected =metricRegistry.meter(
                MetricRegistry.name( "asyncMetricIndexTasks", "rejected" ) );

       received = metricRegistry.meter(
                MetricRegistry.name( "asyncMetricIndexTasks", "received" ) );

       emptyQueueReadCount = metricRegistry.meter(
                MetricRegistry.name( "asyncMetricIndexTasks", "emptyQueueReads" ) );

        queueDepthHistogram = metricRegistry.histogram(
                "asyncMetricIndexTasks.queueDepthHist" );

        queueDepth = metricRegistry.register(
                MetricRegistry.name( "asyncMetricIndexTasks", "queueDepth" ),
                (Gauge<Number>) ( ) -> queueDepthHistogram.getSnapshot().getMean() );


    }

    @Override
    public void run()
    {
        ArrayList<Runnable> batch = new ArrayList<>( batchSize );
        log.info( "queue consumer loop started." );
        // queue consumer loop.
        while ( true )
        {
            try
            {
                if ( stop )
                {
                    break;
                }

                batch.clear();
                if ( queue.drainTo( batch, batchSize ) == 0 )
                {
                    Runnable task = queue.poll( emptyQueuePauseMillis, TimeUnit.MILLISECONDS );
                    if ( null == task )
                    {
                        emptyQueueReadCount.mark();
                        continue;
                    }
                    batch.add( task );
                }
                received.mark( batch.size() );

                batch.forEach( t -> processTask( t ) );
            }
            catch ( Throwable e )
            {
                log.error( "Failure processing metric index tasks.", e );
            }
        }
        log.info( "queue consumer loop stopped." );
    }

    private void processTask( Runnable task )
    {
        try
        {
            task.run();
        }
        catch ( Throwable t )
        {
            log.error( String.format( "Failed to process task: [%s]", task ), t );
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
    public void close()
    {
        log.info( "Stopping " + this );
        stop = true;
        try
        {
            TimeUnit.SECONDS.timedJoin( this, 30 );
            log.info( this + " stopped." );
        }
        catch ( InterruptedException e )
        {
            throw Throwables.propagate( e );
        }

        log.info( this + " stopped." );
    }

    public void accept( final MetricIndexTask t )
    {
        submitted.mark();
        if ( queue.offer( t ) )
        {
            return;
        }
        else
        {
            rejected.mark();
            refreshStats();
        }
    }

    public void dumpStats()
    {
        log.info( String
            .format(
                "stats: submitted=%s, rejected=%s, received=%s, queueDepth=%s, leavesCreated=%s, leavesCreateFailed=%s, emptyQueueReadCount=%s",
                submitted.getCount(), rejected.getCount(), received.getCount(), queueDepth.getValue(),
                MetricIndexTask.leavesCreated.getCount(), MetricIndexTask.leavesFailed.getCount(),
                emptyQueueReadCount.getCount() ) );
    }

    public void refreshStats()
    {
        queueDepthHistogram.update( queue.size() );
    }

}
