/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;


import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;



public class Slot
{
    private static final Logger log = LoggerFactory.getLogger( Slot.class );

    private final ConcurrentHashMap<String, AggregateFunction> metrics = new ConcurrentHashMap<>();
    private final int ts;
    private final LatePointLogger latePointLogger;
    private final int batchSize;
    private final Timer aggregatorFlushTimer;
    private final Meter flushedAggregates;

    private volatile boolean closed = false;

    public Slot(int ts, LatePointLogger latePointLogger, int batchSize, Timer aggregatorFlushTimer,
                Meter flushedAggregates, Meter createdSlots)
    {
        this.ts = ts;
        this.latePointLogger = latePointLogger;
        this.batchSize = batchSize;
        this.aggregatorFlushTimer = aggregatorFlushTimer;
        this.flushedAggregates = flushedAggregates;
        log.info("Created new slot for ts=" + new Date(ts * 1000L));
        createdSlots.mark();
    }

    public boolean isClosed()
    {
        return closed;
    }

    public int size()
    {
        return metrics.size();
    }

    public int getTs() {
        return ts;
    }

    public void apply(MetricAggregate agg, DataPoint m, int now)
    {
        String key = agg.getAggregateName();

        // skip point because this slot has already been closed.
        if( isClosed() )
        {
            latePointLogger.logLatePoint(m, now, LatePointLogger.Reason.SLOT_CLOSED,
                    String.format("aggregatorName: [%s], slot: [%s]", key, this));
            return;
        }
        AggregateFunction f = metrics.computeIfAbsent(key, k -> AggregateFunction.create(k, agg.getAggregationMethod()));
        if (m.name.startsWith("pod222.ecom.bjmr.bjmr_prd") && m.name.endsWith("number-of-filters.max")) {
            log.warn("============");
            log.warn("Received metrics " + m.name);
            log.warn(f.getType() + " " + agg.getAggregationMethod().name() + " " + key);
            log.warn("============");
        }
        f.add(m, now);
    }


    public void close(Consumer<DataPoints> out)
    {
        closed = true;
        flush(out);
    }

    private void flush(Consumer<DataPoints> out)
    {
        int open = 0;
        int flushed = 0;

        final Timer.Context timerContext = aggregatorFlushTimer.time();
        try
        {
            log.info( "Flush: flushing aggregated metrics for time slot " + ts + ", id: " + this.hashCode() );
            List<DataPoint> points = new ArrayList<>(batchSize);
            for ( Map.Entry<String, AggregateFunction> m : metrics.entrySet() )
            {
                AggregateFunction af = m.getValue();
                if (af.getType() == AggregateFunction.Type.SINGLE_VALUE) {
                    DataPoint agg = new DataPoint(m.getKey(), af.apply(), ts);
                    points.add(agg);
                } else {
                    Map<String, Double> aggTypeToValue = af.getValues();
                    for (Map.Entry<String, Double> aggTypeValuePair: aggTypeToValue.entrySet()) {
                        String newAggMetricName = String.format("%s.%s", m.getKey(), aggTypeValuePair.getKey());
                        DataPoint agg = new DataPoint(newAggMetricName, aggTypeValuePair.getValue(), ts);
                        points.add(agg);
                    }
                }
                if ( points.size() >= batchSize )
                {
                    flushed += forward( out, points );
                    points = new ArrayList<>(batchSize);
                }
            }
            if ( points.size() > 0 )
            {
                flushed += forward( out, points );

            }
            flushedAggregates.mark( flushed );
        }
        catch ( Exception e )
        {
            log.error( "Error flushing aggregators. Suppress.", e );
        }
        finally
        {
            long elapsedTimeInNanos = timerContext.stop();
            log.info(String.format("Flush: slot flush for ts=%s took %s (ms). open=%s flushed=%s",
                    ts, TimeUnit.NANOSECONDS.toMillis( elapsedTimeInNanos ), open, flushed ) );
        }
    }


    private int forward( Consumer<DataPoints> out, List<DataPoint> points )
    {
        out.accept( new DataPoints( points ) );
        return points.size();
    }

    @Override
    public String toString() {
        return "Slot{" +
                "ts=" + ts +
                ", hashCode=" + hashCode() +
                '}';
    }
}
