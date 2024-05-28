/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.model.IntervalValues;
import com.demandware.carbonj.service.engine.BlockingPolicy;

/**
 * Creates aggregated data points for low resolution archives. There will be a separate instance per
 * low resolution archive.
 */
public class IntervalProcessorImpl implements IntervalProcessor
{
    private static final Logger log = LoggerFactory.getLogger( IntervalProcessorImpl.class );

    private volatile boolean stop = false;

    // max number of entries to read from the queue and allocate to a single consumer task.
    final private int batchSize;

    // number of consumer tasks per queue (database)
    final private int nConsumers;

    // size of the queue
    final private int queueSize;

    private final ArrayBlockingQueue<IntervalValues> queue;
    private final int emptyQueuePauseInMillis;

    private final Executor s;

    final private IntervalProcessorTaskFactory taskFactory;

    // also used as a logical key to identify this instance of processor.
    final private String dbName;

    public IntervalProcessorImpl(MetricRegistry metricRegistry, String dbName, int batchSize, int queueSize, int nConsumers,
                                 IntervalProcessorTaskFactory taskFactory, int emptyQueuePauseInMillis)
    {
        this.batchSize = batchSize;
        this.queueSize = queueSize;
        this.taskFactory = taskFactory;
        this.nConsumers = nConsumers;
        this.dbName = dbName;

        this.queue = new ArrayBlockingQueue<>( queueSize );
        this.emptyQueuePauseInMillis = emptyQueuePauseInMillis;

        Timer blockingTimer = metricRegistry
                                         .timer( MetricRegistry.name( dbName, "intervalProcessorsQueue", "taskExecutorBlocks" ) );
        this.s = new ThreadPoolExecutor( nConsumers, nConsumers, 1, TimeUnit.HOURS,
                new LinkedBlockingQueue<>(nConsumers * 5),
                new ThreadFactoryBuilder().setNameFormat(dbName + "-interval-processor-thread-%d").build(),
            new BlockingPolicy( blockingTimer ) );
    }

    public String getDbName()
    {
        return dbName;
    }

    /**
     * Puts object on the queue.
     *
     * @param intervalValues queue entry
     * @throws InterruptedException Any interrupted exception
     */
    @Override
    public void put(IntervalValues intervalValues)
        throws InterruptedException
    {
        if( !dbName.equals( intervalValues.dbName ) )
        {
            log.error(String.format("Database name doesn't match. IntervalProcessor dbName [%s], interval [%s]",
                dbName, intervalValues));
            return;
        }
        // will block if the queue is full
        queue.put( intervalValues );
    }

    @Override
    public void run()
    {
        log.info( String.format("Started interval processor for dbName [%s], with queueSize [%s], consumers [%s], batch size [%s]",
            dbName, queueSize, nConsumers, batchSize) );

        // queue consumer loop.
        while ( true )
        {
            try
            {
                if ( stop )
                {
                    return;
                }

                List<IntervalValues> batch = new ArrayList<>( batchSize );
                try
                {
                    queue.drainTo( batch, batchSize );
                    if (batch.isEmpty())
                    {
                        TimeUnit.MILLISECONDS.sleep(emptyQueuePauseInMillis);
                        continue;
                    }
                    Runnable task = taskFactory.create( batch );
                    s.execute( task );
                }
                catch ( Exception e )
                {
                    log.error( "Failure saving metrics for aggregation.", e );
                    throw new RuntimeException(e);
                }
            }
            catch ( Throwable t )
            {
                log.error( "Unhandled error." + this, t );
            }
        }
    }

    @Override
    public void close()
    {
        log.info( "Stopping " + this );
        stop = true;
    }

    @Override
    public String toString()
    {
        return "IntervalProcessor{" +
            ", dbName='" + dbName + '\'' +
            '}';
    }
}
