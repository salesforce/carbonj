/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.queue.QueueProcessor;
import com.salesforce.cc.infra.core.kinesis.Message;
import com.salesforce.cc.infra.core.kinesis.PayloadCodec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestKinesisEventsLogger {

    private static final String EVENT = """
            UMONTP/1.0
            Domain:carbonj
            Payload-Version:1.0

            [ \s
               { \s
                  "time":1535742516669,
                  "ipAddresses":[ \s
                     "216.58.194.206",
                     "2607:f8b0:4005:805:0:0:0:200e"
                  ],
                  "type":"dns",
                  "pod":"sponnusamy-ltm.internal.salesforce.com",
                  "namespace":"sponnusamy-ltm.internal.salesforce.com"
               }
            ]""";

    private List<byte[]> datae;
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @BeforeEach
    public void setUp() {
        datae = new LinkedList<>();
    }

    @Test
    public void testSingleEvent() throws Exception {
        QueueProcessor<byte[]> queueProcessor = new KinesisQueueProcessor(metricRegistry, "test", mockAmazonKinesis(), 1);
        EventsLogger<byte[]> kinesisLogger = new KinesisEventsLogger(metricRegistry, 5, 5, new DropRejectionHandler<>(), queueProcessor, 1, 100);
        byte[] eventBytes = EVENT.getBytes(StandardCharsets.UTF_8);
        kinesisLogger.log(eventBytes);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        byte[] encodedBytes = datae.get(0);
        Message message = PayloadCodec.decode(encodedBytes);
        assertEquals("2.0", message.getHeader("Payload-Version"));
        Collection<byte[]> eventCollection = GzipPayloadV2Codec.getInstance().decode(message.getPayload());
        assertEquals(1, eventCollection.size());
        assertArrayEquals(eventBytes, eventCollection.iterator().next());
    }

    private AmazonKinesis mockAmazonKinesis() {
        AmazonKinesis mockKinesisClient = mock(AmazonKinesis.class);
        when(mockKinesisClient.putRecord(argThat(new PutRecordRequestArgMatcher(datae)))).thenReturn(new PutRecordResult().withShardId("1"));
        return mockKinesisClient;
    }

    private static class PutRecordRequestArgMatcher
            implements ArgumentMatcher<PutRecordRequest>
    {
        private final List<byte[]> dataList;

        PutRecordRequestArgMatcher(List<byte[]> dataList)
        {
            this.dataList = dataList;
        }

        @Override
        public boolean matches(PutRecordRequest argument) {
            byte[] bytes = argument.getData().array();
            return dataList.add(bytes);
        }
    }
}
