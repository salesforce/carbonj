/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPayloadV2 {

    private static final int TOTAL_EVENTS = 5000;
    private static final int EVENTS_PER_PAYLOAD = 50;

    private static final String EVENT_PREFIX = """
            UMONTP/1.0
            Domain:test
            Payload-Version:1.0

            """;

    private static final String EVENT_START =        "[  \n";
    private static final String EVENT_TEMPLATE = """
               { \s
                  "time":%d,
                  "ipAddresses":[ \s
                     "216.58.194.206",
                     "2607:f8b0:4005:805:0:0:0:200e"
                  ],
                  "type":"dns",
                  "taskNo":%d,
                  "eventNo":%d
               }
            """;

    private static final String EVENT_END =  "]";

    @Test
    public void run() throws Exception {
        int noOfTasks = TOTAL_EVENTS / EVENTS_PER_PAYLOAD;
        List<byte[]> eventCollections = new LinkedList<>();
        int originalBytesLength = 0;
        for (int i = 0; i < noOfTasks; i++) {
            String eventCollection = generateEvents(i);
            byte[] eventCollectionBytes = eventCollection.getBytes(StandardCharsets.UTF_8);
            eventCollections.add(eventCollectionBytes);
            originalBytesLength += eventCollectionBytes.length;
        }
        PayloadV2Codec payloadV2Codec = GzipPayloadV2Codec.getInstance();
        byte[] encodedBytes = payloadV2Codec.encode(eventCollections);
        System.out.printf("Length:  Original - %d  Encoded - %d%n", originalBytesLength, encodedBytes.length);
        Collection<byte[]> decodedBytes = payloadV2Codec.decode(encodedBytes);
        assertEquals(noOfTasks, decodedBytes.size());
        List<byte[]> decodedEventCollections = new ArrayList<>(decodedBytes);
        for (int i = 0; i < noOfTasks; i++) {
            assertEquals(eventCollections.get(i).length, decodedEventCollections.get(i).length);
        }
    }

    private String generateEvents(int taskNo) {
        StringBuilder eventBuilder = new StringBuilder();
        eventBuilder.append(EVENT_PREFIX);
        eventBuilder.append(EVENT_START);
        String event = String.format(EVENT_TEMPLATE, System.currentTimeMillis(), taskNo, 0);
        eventBuilder.append(event);
        for (int i = 1; i < TestPayloadV2.EVENTS_PER_PAYLOAD; i++) {
            eventBuilder.append(",");
            event = String.format(EVENT_TEMPLATE, System.currentTimeMillis(), taskNo, i);
            eventBuilder.append(event);
        }
        eventBuilder.append(EVENT_END);
        return eventBuilder.toString();
    }
}
