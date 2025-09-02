/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.BlockingPolicy;
import com.demandware.carbonj.service.engine.InputQueueThreadFactory;
import com.demandware.carbonj.service.queue.QueueProcessor;
import com.google.common.collect.ImmutableMap;
import com.salesforce.cc.infra.core.kinesis.Message;
import com.salesforce.cc.infra.core.kinesis.PayloadCodec;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Processes events placed in general events queue and ships to a kinesis stream in metrics cloud.
 */
public class KinesisQueueProcessor implements QueueProcessor<byte[]>, StatsAware {

    private static Meter kinesisMessagesSent;

    private static Meter logEventsDropped;

    private static Meter messageRetryCounter;

    public static Histogram messageSize;

    private static final Map<String, String> HEADERS = ImmutableMap.of(
            "Payload-Version", "2.0");

    private static final Logger log = LoggerFactory.getLogger(KinesisQueueProcessor.class);

    private static final int NUM_RETRIES = 3;

    private final String streamName;
    private final KinesisClient kinesisClient;
    private final ThreadPoolExecutor ex;

    private final Histogram activeThreadsHistogram;

    private final Gauge<Number> activeThreadCount;
    private final Gauge<Number> taskCount;

    KinesisQueueProcessor(MetricRegistry metricRegistry, String streamName, KinesisClient kinesisClient, int noOfThreads) {
        this.streamName = streamName;
        this.kinesisClient = kinesisClient;

        kinesisMessagesSent = metricRegistry.meter
                (MetricRegistry.name( "events", "kinesis", "sent" ) );

        logEventsDropped = metricRegistry.meter(MetricRegistry.name( "events", "dropped" ) );

        messageRetryCounter = metricRegistry.meter
                (MetricRegistry.name( "events", "putRetry") );

        messageSize = metricRegistry.histogram(
                MetricRegistry.name( "events", "messageSize" ) );


        Timer blockingTimer =
                metricRegistry.timer( MetricRegistry.name( "events",  "processor", "taskExecutorBlocks" ) );
        this.ex =
                new ThreadPoolExecutor( noOfThreads, noOfThreads, 24, TimeUnit.HOURS, new ArrayBlockingQueue<>(
                        5 * noOfThreads ), new InputQueueThreadFactory(  "kinesis-event-task-" ),
                        new BlockingPolicy( "InputQueue",this,  blockingTimer, false ) );

        taskCount = metricRegistry.register(
                MetricRegistry.name("events",  "processor", "taskCount"), ( ) -> ex.getQueue().size() );

        activeThreadsHistogram = metricRegistry.histogram(
                MetricRegistry.name("events",  "processor", "activeThreadsHist" ));

        activeThreadCount = metricRegistry.register(
                MetricRegistry.name( "events",  "processor", "activeThreads" ), ( ) -> activeThreadsHistogram.getSnapshot().getMean() );
    }

    @Override
    public void process(Collection<byte[]> events) {
        ex.submit(new KinesisEventTask(streamName, kinesisClient, events));
    }

    @Override
    public void dumpStats()
    {
        log.info("stats: activePoolThreadCount={}", activeThreadCount.getValue());

        if (taskCount != null) {
            log.info("stats: taskCount={}", taskCount.getValue().intValue());
        }
    }

    @Override
    public void refreshStats()
    {
        activeThreadsHistogram.update( ex.getActiveCount() );
    }

    private static final class KinesisEventTask implements Runnable {

        private final String streamName;
        private final KinesisClient kinesisClient;
        private final Collection<byte[]> events;

        KinesisEventTask(String streamName, KinesisClient kinesisClient, Collection<byte[]> events) {
            this.streamName = streamName;
            this.kinesisClient = kinesisClient;
            this.events = events;
        }

        @Override
        public void run() {
            try {
                byte[] encodedDataBytes = GzipPayloadV2Codec.getInstance().encode(events);
                Message message = new Message(HEADERS, encodedDataBytes);
                byte[] payload = PayloadCodec.encode(message);
                PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                        .streamName(streamName)
                        .data(SdkBytes.fromByteArray(payload))
                        .partitionKey(UUID.randomUUID().toString())
                        .build();
                boolean processedSuccessfully = false;
                for (int i = 0; i < NUM_RETRIES && !processedSuccessfully; i++) {
                    try {
                        kinesisClient.putRecord(putRecordRequest);
                        processedSuccessfully = true;

                        if (log.isDebugEnabled()) {
                            log.debug("Message sent with size: {} for events numbering {}", payload.length, events.size());
                        }

                        messageSize.update(payload.length);
                        kinesisMessagesSent.mark();
                    } catch (ProvisionedThroughputExceededException e) {
                        messageRetryCounter.mark();
                        // waiting out for 1 second to retry put record
                        Thread.sleep(1000L);
                    }
                }
            } catch (Exception e) {
                logEventsDropped.mark();
                log.error("Failed to send log events", e);
            }
        }
    }
}
