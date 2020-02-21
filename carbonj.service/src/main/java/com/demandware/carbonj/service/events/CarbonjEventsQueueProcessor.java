/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.queue.QueueProcessor;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.salesforce.cc.infra.core.kinesis.Message;
import com.salesforce.cc.infra.core.kinesis.PayloadCodec;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

/**
 * This class processes events placed in carbonj events queue.
 */
public class CarbonjEventsQueueProcessor implements QueueProcessor<JsonObject> {

    private final Meter eventsDropped;

    private static final Logger log = LoggerFactory.getLogger(CarbonjEventsQueueProcessor.class);

    private static final Map<String, String> HEADERS = ImmutableMap.of("Domain", "carbonj",
            "Payload-Version", "1.0");

    private final EventsLogger<byte[]> parentEventLogger;


    public CarbonjEventsQueueProcessor(MetricRegistry metricRegistry, EventsLogger<byte[]> parentEventLogger)
    {
        this.parentEventLogger = parentEventLogger;
        this.eventsDropped = metricRegistry.meter(MetricRegistry.name( "cjevents",  "dropped" ) );
    }

    @Override
    public void process(Collection<JsonObject> logEvents)
    {
        try
        {
            JsonArray eventsJsonArray = new JsonArray();
            for (JsonObject eventJson: logEvents) {
                eventsJsonArray.add(eventJson);
            }
            String json = eventsJsonArray.toString();

            Message message = new Message(HEADERS,
                    json.getBytes(StandardCharsets.UTF_8));
            byte[] bytes = PayloadCodec.encode(message);
            parentEventLogger.log(bytes);
        } catch (Exception e) {
            eventsDropped.mark();
            log.error("Failed to send log events", e);
        }
    }
}
