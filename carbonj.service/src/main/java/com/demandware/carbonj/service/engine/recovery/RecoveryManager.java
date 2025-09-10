/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.engine.PointProcessor;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RecoveryManager implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);

    private final MetricRegistry metricRegistry;
    private final GapsTable gapsTable;
    private final String streamName;
    private final PointProcessor pointProcessor;
    private final KinesisClient kinesisClient;
    private final long idleTimeInMillis;
    private final long retryTimeInMillis;
    private final DataPointCodec codec;

    public RecoveryManager(MetricRegistry metricRegistry, GapsTable gapsTable, String streamName, PointProcessor pointProcessor,
                           KinesisClient kinesisClient, long idleTimeInMillis, long retryTimeInMillis,
                           DataPointCodec codec) {
        this.gapsTable = gapsTable;
        this.streamName = streamName;
        this.pointProcessor = pointProcessor;
        this.kinesisClient = kinesisClient;
        this.idleTimeInMillis = idleTimeInMillis;
        this.retryTimeInMillis = retryTimeInMillis;
        this.metricRegistry = metricRegistry;
        this.codec = codec;
    }

    @Override
    public void run() {
        try {
            log.info("Recovery: Running Recovery Manager....");
            List<Gap> gaps = Collections.synchronizedList(gapsTable.getGaps());
            log.info("Recovery: Found gaps {}", gaps);

            KinesisStream kinesisStream = new KinesisStreamImpl(metricRegistry, kinesisClient, streamName,
                    retryTimeInMillis);

            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            executor.setRemoveOnCancelPolicy(true);

            while (!gaps.isEmpty()) {
                Gap gap = gaps.get(0);

                // checkpoint recovery table periodically so that we do not have to restart recovery from the start if
                // carbonj restarts in the middle of recovery.
                Accumulator accumulator = pointProcessor.getAccumulator();
                RecoveryCheckPointCommand gapTableCommand = new RecoveryCheckPointCommand(gapsTable, gap, accumulator);
                ScheduledFuture<?> checkPointCmdFuture = executor.scheduleWithFixedDelay(gapTableCommand, 1, 1,
                        TimeUnit.MINUTES);

                // process gap.
                log.info("Recovery: Recovering gap: {} - {} : LastRecoveryTime: {}", gap.startTime(), gap.endTime(), gap.lastRecovered());
                GapProcessor gapProcessor = new GapProcessor(metricRegistry, gap, kinesisStream, pointProcessor, idleTimeInMillis, codec);
                gapProcessor.process();

                // gap has been processed.  clean up resources.
                checkPointCmdFuture.cancel(false);
                gapsTable.delete(gap);
                if (accumulator != null) {
                    //todo:  ideally we would like to have a separate point processor while proccessing each gap so that
                    // we do not have to clear state between processing each gaps.
                    pointProcessor.flushAggregations(true);  // flush all slots.
                    accumulator.reset();
                }

                gaps = gapsTable.getGaps();
            }

            executor.shutdown();
            gapsTable.destroy();

            log.info("Recovery: Exiting Recovery Manager....");
        } catch (InterruptedException e) {
            log.error("Interrupted..", e);
        } catch (Exception e) {
            log.error("Unexpected error", e);  // todo: add retry logic
        }
    }

    // We check point the recovery to the last slot flushed while processing recovery data points.
    private static class RecoveryCheckPointCommand implements Runnable {

        private final GapsTable gapsTable;
        private final Gap gap;
        private final Accumulator accumulator;

        RecoveryCheckPointCommand(GapsTable gapsTable, Gap gap, Accumulator accumulator) {
            this.gapsTable = gapsTable;
            this.gap = gap;
            this.accumulator = accumulator;
        }

        @Override
        public void run() {
            int maxClosedSlotTs = accumulator == null ? 0 : accumulator.getMaxClosedSlotTs();

            if (maxClosedSlotTs <= 0) {
                return;
            }

            long recoveredSoFarInMillis = maxClosedSlotTs * 1000L;
            try {
                gapsTable.updateGap(new GapImpl(gap.startTime(), gap.endTime(), new Date(recoveredSoFarInMillis)));
            } catch (Exception e) {
                log.error("Exception while updating gaps table", e);
            }
        }
    }
}
