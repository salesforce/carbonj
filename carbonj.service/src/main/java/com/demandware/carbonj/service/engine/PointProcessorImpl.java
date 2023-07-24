/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.*;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PointProcessorImpl implements PointProcessor
{
    private static Logger log = LoggerFactory.getLogger( PointProcessorImpl.class );

    // sample active threads every 10 sec.
    private final Histogram activeThreadsHistogram;

    private final Gauge<Number> activeThreadCount;

    private Gauge<Number> taskCount;

    private final ThreadPoolExecutor ex;

    final String name;

    private PointProcessorTaskBuilder taskBuilder;

    private final PointFilter filter;

    public PointProcessorImpl(MetricRegistry metricRegistry, String name, int threads, PointProcessorTaskBuilder taskBuilder)
    {
        this.name = name;

        this.taskBuilder = Preconditions.checkNotNull(taskBuilder);

        this.filter = taskBuilder.filter;

        if ( 0 == threads )
        {
            log.warn( "Process points on IO threads." );
            ex = null;
        }
        else
        {
            Timer blockingTimer =
                    metricRegistry.timer( MetricRegistry.name( name, "taskExecutorBlocks" ) );
            ex =
                    new ThreadPoolExecutor( threads, threads, 24, TimeUnit.HOURS, new ArrayBlockingQueue<>(
                            5 * threads ), new InputQueueThreadFactory( name + "-task-" ),
                            new BlockingPolicy( "InputQueue",this,  blockingTimer, false ) );

            taskCount = metricRegistry.register(
                    MetricRegistry.name(name, "taskCount"), ( ) -> ex.getQueue().size() );
        }

        activeThreadsHistogram = metricRegistry.histogram(
                MetricRegistry.name(name, "activeThreadsHist" ));

        activeThreadCount = metricRegistry.register(
                activeThreadsGaugeName(), ( ) -> activeThreadsHistogram.getSnapshot().getMean() );
    }

    public void drain()
    {
        DrainUtils.drain( ex );
    }

    @Override
    public void process( List<DataPoint> points )
    {
        log.info("Process the data points");
        Runnable task = taskBuilder.task(points);
        if( ex != null ) {
            ex.submit(task);
        }
        else
        {
            task.run();
        }
    }

    @Override
    public void dumpStats()
    {
        if( filter != null )
        {
            filter.dumpStats();
        }
        log.info( String.format("stats: activePoolThreadCount=%s",  activeThreadCount.getValue() ) );

        if (taskCount != null) {
            log.info( String.format("stats: taskCount=%d",  taskCount.getValue().intValue() ) );
        }
    }

    @Override
    public void refreshStats()
    {
        activeThreadsHistogram.update( activeThreadCount() );
    }

    private int activeThreadCount()
    {
        if ( ex == null )
        {
            return 0;
        }
        else
        {
            return ex.getActiveCount();
        }
    }

    private String activeThreadsGaugeName()
    {
        return MetricRegistry.name( name, "activeThreads" );
    }

    public void close()
    {
        try
        {
            if ( null != ex )
            {
                ex.shutdown();
                ex.awaitTermination( 15, TimeUnit.SECONDS );
                log.info( "point processing tasks stopped." );
            }
        }
        catch ( InterruptedException e )
        {
            throw Throwables.propagate( e );
        }


        if( filter != null )
        {
            filter.close();
        }

    }

    // there should be only one flush running at a time.
    @Override
    public synchronized void flushAggregations(boolean force)
    {
        log.info("Flush: flushing aggregations");
        Consumer<DataPoints> outProcessor = taskBuilder.getOutProcessor();
        Accumulator accumulator = taskBuilder.getAccumulator();
        accumulator.rollUp(outProcessor, System.currentTimeMillis(), force);
        log.info("Flush: done flushing aggregations");
    }

    public Accumulator getAccumulator() {
        return taskBuilder.getAccumulator();
    }

}
