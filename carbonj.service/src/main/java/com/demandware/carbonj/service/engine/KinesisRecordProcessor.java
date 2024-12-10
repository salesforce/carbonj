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
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

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

    private Counter recordsFetchedPerShardCounter;
    private Counter noOfFetchesPerShardCounter;

    private String kinesisShardId;
    private long nextCheckpointTimeInMillis;

    KinesisRecordProcessor(MetricRegistry metricRegistry, PointProcessor pointProcessor, Meter metricsReceived, Meter messagesRecieved,
                           Histogram pointsPerTask, KinesisConfig kinesisConfig, Meter messageRetry,
                           Meter dropped, Meter taskCount, Timer consumerTimer, Histogram latency,
                           DataPointCodec codec, String kinesisStreamName) {

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

        leaseLostCount = metricRegistry.counter(MetricRegistry.name("kinesis", "lostLease"));
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        this.kinesisShardId = initializationInput.shardId();
        log.info("Initializing record processor for shard: " + this.kinesisShardId);

        // metrics to track number of records received per shard.
        MetricRegistry registry = metricRegistry;
        recordsFetchedPerShardCounter = registerCounter(registry,
                MetricRegistry.name("kinesis", kinesisStreamName, this.kinesisShardId, "received"));
        noOfFetchesPerShardCounter = registerCounter(registry,
                MetricRegistry.name("kinesis", kinesisStreamName, this.kinesisShardId, "fetch"));
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
            final Timer.Context timerContext = consumerTimer.time();
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    processSingleRecord(record, receiveTimeStamp);
                    processedSuccessfully = true;
                    break;
                }
                catch (Throwable t) {
                    log.error("Caught throwable while processing record "+ t.getMessage(), t);
                }
                messageRetry.mark();
                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    log.error("Interrupted sleep"+ e.getMessage() ,e);
                }
            }
            if (!processedSuccessfully) {
                dropped.mark();
                log.error("Couldn't process record " + record + ". Skipping the record.");
            }
            else {
                messagesRecieved.mark();
            }
            timerContext.stop();
        }
    }

    private void processSingleRecord(KinesisClientRecord record, long receiveTimeStamp) {
        byte[] array = new byte[record.data().remaining()];
        record.data().get(array);
        DataPoints dataPoints = codec.decode(array);
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
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        log.info("Shutting down record processor for shard: " + kinesisShardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                leaseLostCount.inc();
                log.error("Caught shutdown exception, skipping checkpoint."+se.getMessage(),se);
                break;
            } catch (ThrottlingException e) {
                if (i >= (NUM_RETRIES - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts."+ e.getMessage(),e );
                    break;
                } else {
                    log.error("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES+ e.getMessage(),e );
                }
            } catch (InvalidStateException e) {
                log.error(e.getMessage(), e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                log.error("Interrupted sleep"+e.getMessage(), e);
            }
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.warn("Lease has been lost. No longer able to checkpoint.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            shardEndedInput.checkpointer().checkpoint();
            log.info("Shard completed and checkpoint written.");
        } catch (InvalidStateException | ShutdownException e) {
            log.error("Shard ended. Problem writing checkpoint.", e);
        }
    }
}
