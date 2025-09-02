/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class KinesisRecordProcessor implements ShardRecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(KinesisRecordProcessor.class);

    private final MetricRegistry metricRegistry;

    private static Counter leaseLostCount;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;

    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private final PointProcessor pointProcessor;

    private final Meter metricsReceived;
    private final Meter messagesRecieved;
    private final Meter dropped;
    private final Meter taskCount;
    private final Timer consumerTimer;
    private final Histogram latency;
    private final DataPointCodec codec;
    private final String kinesisStreamName;
    private final KinesisConfig kinesisConfig;
    private final Meter messageRetry;
    private final Histogram pointsPerTask;
    private final CheckPointMgr<Date> checkPointMgr;

    private Counter recordsFetchedPerShardCounter;
    private Counter noOfFetchesPerShardCounter;

    private String kinesisShardId;
    private long nextCheckpointTimeInMillis;

    KinesisRecordProcessor(MetricRegistry metricRegistry, PointProcessor pointProcessor, Meter metricsReceived, Meter messagesRecieved,
                           Histogram pointsPerTask, KinesisConfig kinesisConfig, Meter messageRetry,
                           Meter dropped, Meter taskCount, Timer consumerTimer, Histogram latency,
                           DataPointCodec codec, String kinesisStreamName, CheckPointMgr<Date> checkPointMgr) {

        this.metricRegistry = metricRegistry;
        this.pointProcessor = pointProcessor;
        this.messagesRecieved = messagesRecieved;
        this.metricsReceived = metricsReceived;
        this.pointsPerTask = pointsPerTask;
        this.kinesisConfig = kinesisConfig;
        this.messageRetry = messageRetry;
        this.dropped = dropped;
        this.taskCount = taskCount;
        this.consumerTimer = consumerTimer;
        this.latency = latency;
        this.codec = codec;
        this.kinesisStreamName = kinesisStreamName;
        this.checkPointMgr = checkPointMgr;

        leaseLostCount = metricRegistry.counter(MetricRegistry.name("kinesis", "lostLease"));
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        String shardId = initializationInput.shardId();
        log.info("Initializing record processor for shard: {}", shardId);
        this.kinesisShardId = shardId;
        try {
            this.nextCheckpointTimeInMillis = checkPointMgr.lastCheckPoint().getTime();
        } catch (Exception e) {
            log.error("Failed to check lastCheckPoint - {}", e.getMessage());
            this.nextCheckpointTimeInMillis = System.currentTimeMillis();
        }

        // metrics to track number of records received per shard.
        MetricRegistry registry = metricRegistry;
        recordsFetchedPerShardCounter = registerCounter(registry,
                MetricRegistry.name("kinesis", kinesisStreamName, shardId, "received"));
        noOfFetchesPerShardCounter = registerCounter(registry,
                MetricRegistry.name("kinesis", kinesisStreamName, shardId, "fetch"));
    }

    private Counter registerCounter(MetricRegistry registry, String counterName) {
        registry.remove(counterName);  // remove from registry if already present
        return registry.counter(counterName);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        List<KinesisClientRecord> records = processRecordsInput.records();
        recordsFetchedPerShardCounter.inc(records.size());
        noOfFetchesPerShardCounter.inc();

        processRecordsWithRetries(records);
        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(processRecordsInput.checkpointer());
            nextCheckpointTimeInMillis = System.currentTimeMillis() + kinesisConfig.getCheckPointIntervalMillis();
        }
    }

    private void processRecordsWithRetries(List<KinesisClientRecord> records) {
        long receiveTimeStamp = System.currentTimeMillis();
        for (KinesisClientRecord record : records) {
            try (final Timer.Context ignored = consumerTimer.time()) {
                boolean processedSuccessfully = false;
                for (int i = 0; i < NUM_RETRIES; i++) {
                    try {
                        processSingleRecord(record, receiveTimeStamp);
                        processedSuccessfully = true;
                        break;
                    } catch (Throwable t) {
                        log.error("Caught throwable while processing record {}", t.getMessage(), t);
                    }
                    messageRetry.mark();
                    // backoff if we encounter an exception.
                    try {
                        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                    } catch (InterruptedException e) {
                        log.error("Interrupted sleep {}", e.getMessage(), e);
                    }
                }
                if (!processedSuccessfully) {
                    dropped.mark();
                    log.error("Couldn't process record {}. Skipping the record.", record);
                } else {
                    messagesRecieved.mark();
                }
            }
        }
    }

    private void processSingleRecord(KinesisClientRecord record, long receiveTimeStamp) {
        DataPoints dataPoints = codec.decode(record.data().array());
        List<DataPoint> dataPointList = dataPoints.getDataPoints();

        long latencyTime = receiveTimeStamp - dataPoints.getTimeStamp();
        latency.update(latencyTime);
        int noOfDataPoints = dataPointList.size();
        metricsReceived.mark(noOfDataPoints);
        taskCount.mark();
        pointsPerTask.update(noOfDataPoints);

        pointProcessor.process(dataPointList);
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        leaseLostCount.inc();
        log.warn("Lease lost for shard: {}", kinesisShardId);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        log.info("Shard ended for shard: {}. Checkpointing...", kinesisShardId);
        checkpoint(shardEndedInput.checkpointer());
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Shutdown requested for shard: {}. Checkpointing...", kinesisShardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                if (checkPointMgr != null && kinesisConfig.isRecoveryEnabled() && !kinesisConfig.isAggregationEnabled()) {
                    // Only when the aggregation is disabled, and recovery is enabled, we set check point from here
                    // This is a use case in observability platform
                    // Make the checkpoint time slot starting at 0th second every minute
                    checkPointMgr.checkPoint(new Date(nextCheckpointTimeInMillis / 60000 * 60000));
                }
                break;
            } catch (ShutdownException se) {
                leaseLostCount.inc();
                log.error("Caught shutdown exception, skipping checkpoint. {}", se.getMessage(), se);
                break;
            } catch (ThrottlingException e) {
                if (i >= (NUM_RETRIES - 1)) {
                    log.error("Checkpoint failed after {} attempts. {}", i + 1, e.getMessage(), e);
                    break;
                } else {
                    log.error("Transient issue when checkpointing - attempt {} of " + NUM_RETRIES + "{}", i + 1, e.getMessage(), e);
                }
            } catch (InvalidStateException e) {
                log.error(e.getMessage(), e);
                break;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                log.error("Interrupted sleep {}", e.getMessage(), e);
            }
        }
    }
}

