/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.codahale.metrics.Meter;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class PointProcessorTask implements Runnable
{
    private static final Logger log = LoggerFactory.getLogger( PointProcessorTask.class );

    private static Meter failedPoints;


    private final Timer taskTimer;


    private List<DataPoint> points;
    private MetricList blacklist;

    private MetricList allowOnly;

    final Accumulator accumulator;

    final Relay auditLog;

    final boolean aggregationEnabled;

    private final PointFilter filter;

    final Consumer<DataPoints> out;

    private final NamespaceCounter nsCounter;

    public PointProcessorTask(MetricRegistry metricRegistry, List<DataPoint> points, MetricList blacklist, MetricList allowOnly, Accumulator accumulator, boolean aggregationEnabled,
                              PointFilter pointFilter, Consumer<DataPoints> out, Relay auditLog, NamespaceCounter nsCounter)
    {
        this.points = points;
        this.blacklist = blacklist;
        this.allowOnly = allowOnly;
        this.accumulator = accumulator;
        this.aggregationEnabled = aggregationEnabled;
        this.filter = pointFilter;
        this.out = out;
        this.auditLog = auditLog;
        this.nsCounter = Preconditions.checkNotNull(nsCounter);

        this.failedPoints = metricRegistry.meter(
                MetricRegistry.name( "aggregator", "failedPoints" ) );


        this.taskTimer = metricRegistry.timer(
                MetricRegistry.name( "inputQueue", "taskTimer" ) );

    }

    @Override
    public void run()
    {
        final Timer.Context timerContext = taskTimer.time();
        try
        {
            process( points );
        }
        finally
        {
            long elapsedTimeInNanos = timerContext.stop();
            if( log.isDebugEnabled() )
            {
                log.debug( "Input queue processing task took " + TimeUnit.NANOSECONDS.toMillis( elapsedTimeInNanos ) + "ms." );
            }
        }
    }

    void process( List<DataPoint> points )
    {
        for ( DataPoint p : points )
        {
            try
            {
                processSinglePoint( p );
            }
            catch ( Throwable t )
            {
                failedPoints.mark();
                log.error( String.format( "Failed to process point: [%s]", p ), t );
                p.drop();
            }
        }

        forward( points );
    }

    private void processSinglePoint( DataPoint t )
    {
        if ( log.isTraceEnabled() )
        {
            log.trace( "->" + t );
        }
        nsCounter.count( t.name );
        log.info("Process single data point: " + t.name);

        auditLog.accept( t );

        // filter is cheaper so it goes first.
        if ( filter != null && !filter.accept( t ) )
        {
            t.drop();
            return;
        }

        // If we have a non-empty allow list, drop everything not on the list (before we handle blocking)
        if ( allowOnly != null && !allowOnly.isEmpty() && !allowOnly.match( t.name ) )
        {
            t.drop();
            return;
        }

        if ( blacklisted( t ) )
        {
            if ( log.isDebugEnabled() )
            {
                log.debug( String.format( "Dropping blacklisted data point [%s]", t ) );
            }
            t.drop();
            return;
        }

        if ( aggregationEnabled )
        {
            accumulator.add( t );
        }

    }

    private boolean blacklisted( DataPoint t )
    {
        return blacklist.match( t.name );
    }

    private void forward( List<DataPoint> points )
    {
        if ( log.isTraceEnabled() )
        {
            log.trace( "<-" + points );
        }
        out.accept( new DataPoints( points ) );
    }

}
