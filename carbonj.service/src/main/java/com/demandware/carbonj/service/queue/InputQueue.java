/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.queue;

import com.codahale.metrics.*;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.DrainUtils;
import com.demandware.carbonj.service.engine.RejectionHandler;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class InputQueue<T> extends Thread implements Consumer<T>, StatsAware, Closeable
{
    private static final Logger log = LoggerFactory.getLogger( InputQueue.class );

    private final Meter rejects;

    // sample queue depth every 10 sec.
    private final Histogram queueDepthHistogram;

    private final Gauge<Number> queueDepth;

    private final Meter received;

    private final Meter taskCount;

    private final Meter emptyQueueReadCount;

    private final Histogram pointsPerTask;
    private final long maxWaitTimeInMillis;

    private final QueueProcessor<T> pointProcessor;

    private final int batchSize;

    private final BlockingQueue<T> queue;

    private volatile boolean stop = false;

    private final long emptyQueuePauseMillis;

    private final RejectionHandler<T> rh;

    public InputQueue(MetricRegistry metricRegistry, String name, QueueProcessor<T> queueProcessor, int queueSize,
                      RejectionHandler<T> rejectionHandler, int batchSize,
                      long emptyQueuePauseMillis, long maxWaitTimeInMillis)
    {
        this.pointProcessor = Preconditions.checkNotNull(queueProcessor);
        this.batchSize = batchSize;
        this.emptyQueuePauseMillis = emptyQueuePauseMillis;
        this.queue = new ArrayBlockingQueue<>( queueSize );
        this.rh = rejectionHandler;

        rejects = metricRegistry.meter(
                MetricRegistry.name( name, "inputQueue", name, "rejects" ) );

        // sample queue depth every 10 sec.
        queueDepthHistogram = metricRegistry.histogram( MetricRegistry.name(name, "inputQueue", name, "queueDepthHist" ));

        queueDepth = metricRegistry.register( queueSizeGaugeName(name),
                (Gauge<Number>) ( ) -> queueDepthHistogram.getSnapshot().getMean() );

        received = metricRegistry.meter(
                MetricRegistry.name( name, "inputQueue", name, "received" ) );

        taskCount = metricRegistry.meter(
                MetricRegistry.name( name, "inputQueue", name, "taskCount" ) );

        emptyQueueReadCount = metricRegistry.meter(
                MetricRegistry.name( name, "inputQueue", name, "emptyQueueReadCount" ) );

        pointsPerTask = metricRegistry.histogram(
                MetricRegistry.name( name, "inputQueue", name, "pointsPerTask" ) );
        this.maxWaitTimeInMillis = maxWaitTimeInMillis;
    }

    public void drain()
    {
        DrainUtils.drain( queue );
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

                List<T> batch = new ArrayList<>( batchSize );

                int remainingBatchSize = batchSize;
                long maxWaitTime = System.currentTimeMillis() + maxWaitTimeInMillis;

                while (remainingBatchSize > 0 && System.currentTimeMillis() < maxWaitTime) {
                    if (queue.drainTo(batch, remainingBatchSize) == 0) {
                        T point = queue.poll(emptyQueuePauseMillis, TimeUnit.MILLISECONDS);
                        if (null == point) {
                            emptyQueueReadCount.mark();
                            continue;
                        }
                        batch.add(point);
                    }
                    remainingBatchSize = batchSize - batch.size();
                }

                int noOfDataPoints = batch.size();
                if (noOfDataPoints > 0)
                {
                    try
                    {
                        taskCount.mark();
                        pointsPerTask.update(batch.size());
                        pointProcessor.process(batch);
                    } catch (Throwable t)
                    {
                        log.error("Error when processing batch of points", t);
                    }
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
    public void close()
    {
        log.info( "Stopping " + this );

        stop = true;
        log.info( this + " stopped." );
    }

    @Override
    public void accept( final T t )
    {
        received.mark();

        if ( queue.offer( t ) )
        {
            return;
        }

        rejects.mark();
        rh.rejected( queue, t );
    }


    @Override
    public void dumpStats()
    {
        log.info( String.format(
            "stats: rejects=%s queueSize=%s",
            rejects.getCount(), queueDepth.getValue()) );
    }

    public void refreshStats()
    {
        queueDepthHistogram.update( queuedItemsCount() );
    }

    public int queuedItemsCount()
    {
        return queue.size();
    }

    private String queueSizeGaugeName(String name)
    {
        return MetricRegistry.name( "inputQueue", name, "queueSize" );
    }
}
