/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.*;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

public class KinesisRecordProcessor implements IRecordProcessor {

    private static Logger log = LoggerFactory.getLogger(KinesisRecordProcessor.class);

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

    public void initialize(String shardId) {
        log.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;

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

    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        recordsFetchedPerShardCounter.inc(records.size());
        noOfFetchesPerShardCounter.inc();

        processRecordsWithRetries(records);
        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + kinesisConfig.getCheckPointIntervalMillis();
        }
    }

    private void processRecordsWithRetries(List<Record> records) {
        long receiveTimeStamp = System.currentTimeMillis();
        for (Record record : records) {
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

    private void processSingleRecord(Record record, long receiveTimeStamp) {
        ByteBuffer data = record.getData();
        DataPoints dataPoints = codec.decode(data.array());
        List<DataPoint> dataPointList = dataPoints.getDataPoints();

        long latencyTime = receiveTimeStamp - dataPoints.getTimeStamp();
        latency.update(latencyTime);
        int noOfDataPoints = dataPointList.size();
        metricsReceived.mark(noOfDataPoints);
        taskCount.mark();
        pointsPerTask.update(noOfDataPoints);

        pointProcessor.process(dataPointList);
    }

    // @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        log.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                if (checkPointMgr != null && kinesisConfig.isRecoveryEnabled() && !kinesisConfig.isAggregationEnabled()) {
                    // Only when the aggregation is disabled, and recovery is enabled, we set check point from here
                    // This is a use case in observability platform
                    checkPointMgr.checkPoint(new Date(nextCheckpointTimeInMillis));
                }
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
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                log.error("Interrupted sleep"+e.getMessage(), e);
            }
        }
    }
}

