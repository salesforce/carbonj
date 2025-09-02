/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKinesisProducerTask {
    @Test
    public void test() {
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter sent = metricRegistry.meter(MetricRegistry.name("kinesis", "sent"));
        Meter drop = metricRegistry.meter(MetricRegistry.name("kinesis", "drop"));
        Meter messagesSent = metricRegistry.meter(MetricRegistry.name("kinesis", "messagesSent"));
        Histogram messageSize = metricRegistry.histogram(MetricRegistry.name("kinesis", "messageSize"));
        Histogram dataPointsPerMessage = metricRegistry.histogram(MetricRegistry.name("kinesis", "dataPointsPerMessage"));
        KinesisProducerTask kinesisProducerTask = new KinesisProducerTask(metricRegistry, mockKinesis(), "test-stream",
                List.of(new DataPoint("foo.bar", 123, 0, false)),
                sent,
                drop,
                messagesSent,
                messageSize,
                metricRegistry.meter(MetricRegistry.name("kinesis", "putRetry")),
                metricRegistry.timer(MetricRegistry.name("kinesis", "producer")).time(),
                dataPointsPerMessage,
                new GzipDataPointCodec());
        kinesisProducerTask.run();
        assertEquals(1, messageSize.getCount());
        assertEquals(1, sent.getCount());
        assertEquals(0, drop.getCount());
        assertEquals(1, dataPointsPerMessage.getCount());
        assertEquals(1, messagesSent.getCount());
    }

    private static KinesisClient mockKinesis() {
        KinesisClient mock = Mockito.mock(KinesisClient.class);
        Mockito.when(mock.putRecord(Mockito.any(PutRecordRequest.class)))
                .thenReturn(PutRecordResponse.builder().shardId("1").build());
        return mock;
    }
}
