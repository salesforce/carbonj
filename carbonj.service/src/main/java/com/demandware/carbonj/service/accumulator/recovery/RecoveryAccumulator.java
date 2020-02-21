/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator.recovery;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.accumulator.*;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RecoveryAccumulator implements StatsAware, Accumulator
{
    private static final Logger log = LoggerFactory.getLogger( RecoveryAccumulator.class );

    private final MetricRegistry metricRegistry;

    private static final int MIN_PAUSE_BETWEEN_FLUSHES_SEC = 15;

    // tracks time it took to evaluate all metrics in all slots.
    private final Timer flushTimer;
    private final NamespaceCounter ns;

    // track number of closed slots.
    private final Meter closedSlots;

    // track number of open slots.
    private final Meter openSlots;

    // sample every 10 sec.
    private final Histogram pendingAggregatesHistogram;

    private final Gauge<Number> pendingAggregatesCount;

    // tracks time it took to flush one slot
    private final Timer aggregatorFlushTimer;

    // tracks number of aggregates that were flushed (closed)
    private final  Meter flushedAggregates;

    private final  Meter createdSlots;

    private final int batchSize;

    private final int recoverySlotMaxLifeSec;

    private final String name;
    private final MetricAggregationPolicyProvider aggregationPolicyProvider;

    private final LatePointLogger latePointLogger;

    private Map<Integer, Slot> slots = new ConcurrentHashMap<>();

    // the max slot created so far..
    private volatile int maxRecoverySlotTs;

    // the max slot that we have closed so far..
    private volatile int maxClosedSlotTs;

    /*
     *   Can be accessed by different threads but only one thread will be running rollUp at a time.
     */
    private volatile long lastRollUp = 0;

    // algorithm that decides which metric goes to which slot
    private final SlotStrategy slotStrategy;

    public RecoveryAccumulator(MetricRegistry metricRegistry, String name, MetricAggregationPolicyProvider aggregateProvider, int batchSize,
                               int recoveryMaxSlots, SlotStrategy slotStrategy, LatePointLogger latePointLogger,
                               NamespaceCounter ns)
    {
        this.name = name;
        this.aggregationPolicyProvider = Preconditions.checkNotNull( aggregateProvider );
        this.batchSize = batchSize;
        this.recoverySlotMaxLifeSec = recoveryMaxSlots * 60;
        this.slotStrategy = slotStrategy;
        this.latePointLogger = latePointLogger;
        this.metricRegistry = metricRegistry;

        // tracks time it took to evaluate all metrics in all slots.
        flushTimer = metricRegistry.timer(
                MetricRegistry.name( name, "flushTimer" ) );

        this.ns = ns;

        // track number of closed slots.
        closedSlots = meter( "closedSlots" );

        // track number of open slots.
        openSlots = meter(  "openSlots" );

        // sample every 10 sec.
        pendingAggregatesHistogram = metricRegistry.histogram(
                MetricRegistry.name( name,"pendingAggregatesHistogram" ));

        pendingAggregatesCount = metricRegistry.register(
                MetricRegistry.name( name, "pendingAggregates" ),
                 ( ) -> pendingAggregatesHistogram.getSnapshot().getMean() );

        // tracks time it took to flush one slot
        aggregatorFlushTimer = metricRegistry.timer(
                MetricRegistry.name( name,  "slotFlushTimer" ) );

        // tracks number of aggregates that were flushed (closed)
        flushedAggregates = metricRegistry.meter(
                MetricRegistry.name( name,  "aggregates" ) );

        createdSlots = metricRegistry.meter(
                MetricRegistry.name( name,  "slotCreated" ) );
    }

    public void add( DataPoint m )
    {
        MetricAggregationPolicy policy = aggregationPolicyProvider.metricAggregationPolicyFor( m.name );

        // one metric can map to 0..N aggregates
        List<MetricAggregate> aggregates = policy.getAggregates();

        // no aggregates for this metric - done.
        if( aggregates.size() == 0 )
        {
            return;
        }

        int slotTs = slotStrategy.getSlotTs(m.ts);

        // check if point arrived too late and this slot has already been closed
        if ( isLate(slotTs) )
        {
            latePointLogger.logLatePoint(m, maxRecoverySlotTs, LatePointLogger.Reason.SLOT_EXPIRED,
                    String.format("metric ts [%s] slot expiration: [%s]", new Date(m.ts * 1000L), new Date(getRecoverySlotExpTimeInSecs()*1000L)));
            return;
        }

        maxRecoverySlotTs = Math.max(slotTs, maxRecoverySlotTs);

        // one metric can map to multiple aggregates
        for (MetricAggregate agg : aggregates)
        {
            if ( agg.isDropOriginal() )
            {
                m.drop();
            }

            ns.count(agg.getAggregateName());

            Slot s = slots.computeIfAbsent(slotTs, k -> new Slot(k, latePointLogger, batchSize, aggregatorFlushTimer,
                    flushedAggregates, createdSlots) );
            s.apply(agg, m, maxRecoverySlotTs);
        }
    }

    private int getRecoverySlotExpTimeInSecs() {
        return maxRecoverySlotTs - recoverySlotMaxLifeSec;
    }

    private boolean isLate(int slotTs) {
        return slotTs < getRecoverySlotExpTimeInSecs();
    }



    /**
     * Flushes (closes) aggregates that are ready to be flushed.
     *
     * @param out                   destination for aggregate values.
     * @param rollUpTimeInMillis    time stamp for this roll up.
     * @param force                 if true,  flush all the slots without waiting for expiration.
     */
    @Override
    public void rollUp( Consumer<DataPoints> out, long rollUpTimeInMillis, boolean force )
    {
        if ( !force && lastRollUp + MIN_PAUSE_BETWEEN_FLUSHES_SEC * 1000 > rollUpTimeInMillis )
        {
            // not enough time passed since the last run.
            return;
        }

        log.info("rollUp: all slots flush started");

        int nClosed = 0;
        int nOpen = 0;
        final Timer.Context timerContext = flushTimer.time();
        try
        {
            for ( Integer slotTs : new ArrayList<>( slots.keySet() ) )
            {
                if (force || canClose(slotTs) )
                {
                    // slot is too old - flushing all metrics in the slot and removing the slot.
                    Slot slot = slots.remove(slotTs);

                    if ( null == slot )
                    {
                        continue;
                    }

                    maxClosedSlotTs = Math.max(slotTs, maxClosedSlotTs);
                    log.info("Max closed slot ts " + new Date(maxClosedSlotTs * 1000L));

                    slot.close(out);
                    closedSlots.mark();
                    log.info(String.format("Flush: closed aggregations slot: [%s], Current Recovery Slot: [%s]",
                            slot, new Date(maxRecoverySlotTs * 1000L)));
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
                    "rollUp: all slots flush completed in %s (ms). Closed slots: %s, Open slots: %s",
                    TimeUnit.NANOSECONDS.toMillis( elapsedTimeInNanos ), nClosed, nOpen ) );
            openSlots.mark(nOpen);
            closedSlots.mark(nClosed);
            lastRollUp = rollUpTimeInMillis;
        }
    }

    private boolean canClose(int slotTs) {
        return slotTs < getRecoverySlotExpTimeInSecs();
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

    private  Meter meter(String name)
    {
        return metricRegistry.meter( MetricRegistry.name( this.name, name ) );
    }

    public int getMaxClosedSlotTs() {
        return maxClosedSlotTs;
    }

    @Override
    public SlotStrategy getSlotStrategy() {
        return slotStrategy;
    }

    @Override
    public void reset() {
        maxRecoverySlotTs = maxClosedSlotTs = 0;
        lastRollUp = 0;
        slots = new ConcurrentHashMap<>();
    }

    // for testing purposes only
    Set<Integer> getTimeSlots() {
        return slots.keySet();
    }
}
