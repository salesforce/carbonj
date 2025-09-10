/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KinesisProducerTask
        implements Runnable
{
    private static final Logger log = LoggerFactory.getLogger( KinesisProducerTask.class );

    private final MetricRegistry metricRegistry;

    private final KinesisClient kinesisClient;

    final private String streamName;

    private final List<DataPoint> points;

    private final Meter metricsSent;

    private final Meter metricsDropped;

    private final Meter messagesSent;

    private final Meter messageRetryCounter;

    private final Histogram messageSize;

    private final Timer.Context timerContext;

    private final Map<String, Counter> shardIdMap;

    private final Histogram dataPointsPerMessage;

    private final DataPointCodec codec;

    private static final int NUM_RETRIES = 3;

    KinesisProducerTask(MetricRegistry metricRegistry, KinesisClient kinesisClient, String streamName, List<DataPoint> points, Meter metricsSent,
                        Meter metricsDropped, Meter messagesSent, Histogram messageSize, Meter messageRetryCounter,
                        Timer.Context timerContext, Histogram dataPointsPerMessage, DataPointCodec codec) {
        this.metricRegistry = metricRegistry;
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.points = points;
        this.metricsSent = metricsSent;
        this.metricsDropped = metricsDropped;
        this.messageSize = messageSize;
        this.messagesSent = messagesSent;
        this.timerContext = timerContext;
        this.messageRetryCounter = messageRetryCounter;
        this.dataPointsPerMessage = dataPointsPerMessage;
        this.codec = codec;
        shardIdMap = new HashMap<>();
    }

    @Override
    public void run() {
        try {
            /* gzip multiple data points*/
            byte[] message = codec.encode(new DataPoints(points, System.currentTimeMillis()));
            PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                    .streamName(streamName)
                    .data(SdkBytes.fromByteArray(message))
                    .partitionKey(UUID.randomUUID().toString())
                    .build();
            boolean processedSuccessfully = false;
            PutRecordResponse putRecordResult = null;
            for (int i = 0; i < NUM_RETRIES && !processedSuccessfully; i++) {
                try {
                    putRecordResult = kinesisClient.putRecord(putRecordRequest);
                    processedSuccessfully = true;
                } catch (ProvisionedThroughputExceededException e) {
                    messageRetryCounter.mark();
                    // waiting out for 1 second to retry put record
                    Thread.sleep(1000L);
                }
            }

            if (processedSuccessfully) {
                messageSize.update(message.length);
                int noOfDataPoints = points.size();
                metricsSent.mark(noOfDataPoints);
                dataPointsPerMessage.update(noOfDataPoints);
                messagesSent.mark();
                if (!shardIdMap.containsKey(putRecordResult.shardId())) {
                    shardIdMap.put(putRecordResult.shardId(), metricRegistry.counter(MetricRegistry.name(streamName, putRecordResult.shardId())));
                }
                shardIdMap.get(putRecordResult.shardId()).inc();
                // log.info("Message sent.ShardId is " + putRecordResult.getShardId());
            } else {
                metricsDropped.mark();
                log.error("Couldn't process record {}. Skipping the record.", putRecordRequest);
            }
        } catch(Throwable e) {
            log.error(e.getMessage(),e);
            metricsDropped.mark();
        } finally {
            timerContext.stop();
        }
    }
}
