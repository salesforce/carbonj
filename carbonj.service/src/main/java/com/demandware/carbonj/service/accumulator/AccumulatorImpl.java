/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.codahale.metrics.*;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.google.common.base.Preconditions;

public class AccumulatorImpl implements StatsAware, Accumulator
{
    private static final Logger log = LoggerFactory.getLogger( AccumulatorImpl.class );

    private static MetricRegistry metricRegistry;

    // tracks time it took to evaluate all metrics in all slots.
    private final Timer flushTimer;

    // track number of closed slots.
    private final Meter closedSlots;

    // track number of open slots.
    private final Meter openSlots;

    // tracks time it took to flush one slot
    private final Timer aggregatorFlushTimer;

    // tracks number of aggregates that were flushed (closed)
    private final Meter flushedAggregates;

    private final Meter createdSlots;

    // sample every 10 sec.
    private final Histogram pendingAggregatesHistogram;

    private final Gauge<Number> pendingAggregatesCount;

    private static final int MIN_PAUSE_BETWEEN_FLUSHES_SEC = 15;

    private final int batchSize;

    private final SlotStrategy slotStrategy;
    private final NamespaceCounter ns;

    private final int slotMaxLifeSec;

    private final int slotRemoveAfterSec;

    private final MetricAggregationPolicyProvider aggregationPolicyProvider;

    private final LatePointLogger latePointLogger;

    private final ConcurrentHashMap<Integer, Slot> slots = new ConcurrentHashMap<>();

    private volatile int maxClosedSlotTs;

    public AccumulatorImpl(MetricRegistry metricRegistry, MetricAggregationPolicyProvider aggregateProvider, int batchSize, int slotMaxLifeSec,
                           SlotStrategy slotStrategy, NamespaceCounter ns)
    {
        this.aggregationPolicyProvider = Preconditions.checkNotNull( aggregateProvider );
        this.batchSize = batchSize;
        this.slotStrategy = slotStrategy;
        this.ns = ns;
        this.latePointLogger = new LatePointLoggerImpl(metricRegistry);
        this.slotMaxLifeSec = slotMaxLifeSec;
        this.slotRemoveAfterSec = slotMaxLifeSec + 5;
        this.metricRegistry = metricRegistry;
        this.flushTimer = metricRegistry.timer(
                MetricRegistry.name( "aggregator", "flushTimer" ) );
        this.aggregatorFlushTimer = metricRegistry.timer(
                MetricRegistry.name( "aggregator", "slotFlushTimer" ) );
        log.info("Accumulator created: " + this.hashCode());
        this.flushedAggregates = metricRegistry.meter(
                MetricRegistry.name( "aggregator", "aggregates" ) );
        this.createdSlots = metricRegistry.meter(
                MetricRegistry.name( "aggregator", "slotCreated" ) );
        this.pendingAggregatesHistogram = metricRegistry.histogram(
                "pendingAggregatesHistogram" );
        this.pendingAggregatesCount = metricRegistry.register(
                MetricRegistry.name( "aggregator", "pendingAggregates" ),
                (Gauge<Number>) ( ) -> pendingAggregatesHistogram.getSnapshot().getMean() );

        closedSlots = metricRegistry.meter( MetricRegistry.name( "aggregator", "closedSlots" ) );

        // track number of open slots.
        openSlots = metricRegistry.meter( MetricRegistry.name( "aggregator", "openSlots" ) );
    }

    @Override
    public void add(DataPoint m)
    {

        add(m, System.currentTimeMillis());

    }

    void add( DataPoint m, long currentTimeInMillis )
    {

        log.info("Adding data points");
        MetricAggregationPolicy policy = aggregationPolicyProvider.metricAggregationPolicyFor( m.name );

        // one metric can map to 0..N aggregates
        List<MetricAggregate> aggregates = policy.getAggregates();
        log.info("Get the aggregates list size: " + aggregates.size());

        // no aggregates for this metric - done.
        if( aggregates.size() == 0 )
        {
            return;
        }

        int slotTs = slotStrategy.getSlotTs(m.ts);
        log.info("Get the slot value: " + slotTs);

        int now = Math.toIntExact(currentTimeInMillis / 1000);
        log.info("Get the current time: " + now);

        // check if point arrived too late and this slot has already been closed
        if ( isLate(now, slotTs) )
        {
            log.info("The point arrived late so slot is closed");
            latePointLogger.logLatePoint(m, now, LatePointLogger.Reason.SLOT_EXPIRED,
                    String.format("slot expiration: [%s]", slotTs + slotMaxLifeSec));
            return;
        }

        // one metric can map to multiple aggregates
        for (MetricAggregate agg : aggregates)
        {
            log.info("Retrieve each aggregate");
            if ( agg.isDropOriginal() )
            {
                log.info("Dropping the metric");
                m.drop();
            }

            ns.count(agg.getAggregateName());

            Slot s = slots.computeIfAbsent(slotTs, k -> new Slot(k, latePointLogger, batchSize, aggregatorFlushTimer,
                    flushedAggregates, createdSlots) );
            s.apply(agg, m, now);
            log.info("Compute the value for slot");
        }
        log.info("Completed adding the data points");

    }

    private boolean isLate(int now, int slotTs) {
        log.info("Check if the point is arriving late");
        return now > slotTs + slotMaxLifeSec;
    }

    /*
     *   Can be accessed by different threads but only one thread will be running rollUp at a time.
     */
    private volatile long lastRollUp = 0;

    /**
     * Flushes (closes) aggregates that are ready to be flushed.
     *
     * @param out                   destination for aggregate values.
     * @param rollUpTimeInMillis    time stamp for this roll up.
     * @param force                 force flush all the slots.  Do not wait for expiration.
     */
    @Override
    public void rollUp(Consumer<DataPoints> out, long rollUpTimeInMillis, boolean force)
    {
        if ( !force && lastRollUp + MIN_PAUSE_BETWEEN_FLUSHES_SEC * 1000 > rollUpTimeInMillis )
        {
            // not enough time passed since the last run.
            return;
        }

        log.info("Flush: all slots flush started");

        int nClosed = 0;
        int nOpen = 0;
        final Timer.Context timerContext = flushTimer.time();
        try
        {
            int rollUpTimeInSecs = Math.toIntExact(rollUpTimeInMillis / 1000);
            for ( Integer slotTs : new ArrayList<>( slots.keySet() ) )
            {
                if (force || canClose(slotTs, rollUpTimeInSecs) )
                {
                    // slot is too old - flushing all metrics in the slot and removing the slot.
                    Slot slot = slots.remove(slotTs);

                    if ( null == slot )
                    {
                        continue;
                    }

                    maxClosedSlotTs = Math.max(slotTs, maxClosedSlotTs);

                    slot.close(out);
                    closedSlots.mark();
                    log.info(String.format("Flush: closed aggregations slot: [%s], now: [%s], slot expiration: [%s]",
                            slot, rollUpTimeInMillis, slotTs + slotRemoveAfterSec));
                    nClosed++;
                }
                else
                {
                    openSlots.mark();
                }
            }
        }
        finally
        {
            long elapsedTimeInNanos = timerContext.stop();
            log.info(String.format(
                    "Flush: all slots flush completed in %s (ms). Closed slots: %s, Open slots: %s",
                    TimeUnit.NANOSECONDS.toMillis( elapsedTimeInNanos ), nClosed, nOpen ) );
            openSlots.mark(nOpen);
            closedSlots.mark(nClosed);
            lastRollUp = rollUpTimeInMillis;
        }
    }

    @Override
    public int getMaxClosedSlotTs() {
        return maxClosedSlotTs;
    }

    private boolean canClose(int slotTs, long rollUpTimeInSecs) {
        return slotTs + slotRemoveAfterSec < rollUpTimeInSecs;
    }

    @Override
    public void dumpStats()
    {
        log.info( String.format(
            "stats: flushedAggregates=%s, pendingAggregates=%s",
                flushedAggregates.getCount(),
                pendingAggregatesCount.getValue()) );
    }

    @Override
    public void refreshStats()
    {
        pendingAggregatesHistogram.update( pendingAggregatesCount() );
    }

    private int pendingAggregatesCount()
    {
        int cnt = 0;
        for ( Slot slot : slots.values() )
        {
            cnt += slot.size();
        }
        return cnt;
    }

    private static Meter meter(String name)
    {
        return metricRegistry.meter( MetricRegistry.name( "aggregator", name ) );
    }

    @Override
    public SlotStrategy getSlotStrategy() {
        return slotStrategy;
    }

    @Override
    public void reset() {
        // not used.
    }
}
