/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.log.CompletedQueryStats;
import com.demandware.carbonj.service.db.log.QueryStats;
import com.demandware.carbonj.service.engine.Query;
import com.demandware.carbonj.service.queue.QueueProcessor;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCarbonjEventsLogger {
    MetricRegistry metricRegistry = new MetricRegistry();

    @Test
    public void testBasic() throws Exception {
        CompletedQueryStats stats =
                new CompletedQueryStats(
                        new Query("randomQuery", 5, 10, 25, System.currentTimeMillis()),
                        20, false, System.currentTimeMillis(), new QueryStats());
        MockQueueProcessor queueProcessor = new MockQueueProcessor();

        EventsLogger<CarbonjEvent> logger = new CarbonJEventsLogger(metricRegistry, 25, 1000, new DropRejectionHandler<>(), queueProcessor,
                "carbonj-p1-0.carbonj-p1.carbonj-a.svc.cluster.local", 100);

        logger.log(stats);

        Thread.sleep(2000);

        Collection<JsonObject> logEvents = queueProcessor.logEvents;
        assertEquals(1, logEvents.size());
        JsonObject logEvent = logEvents.iterator().next();

        assertEquals(Constants.QUERY_LOG_TYPE, logEvent.get("type").getAsString());
        // Assert.assertEquals("carbonj", logEvent.get("domain").getAsString());
        assertEquals("carbonj-p1-0", logEvent.get("pod").getAsString());
        assertEquals("carbonj-a", logEvent.get("namespace").getAsString());
        assertEquals(20, logEvent.get("noOfSeries").getAsInt());
    }

    public static class MockQueueProcessor implements QueueProcessor<JsonObject> {

        Collection<JsonObject> logEvents;

        @Override
        public void process(Collection<JsonObject> items) {
            this.logEvents = items;
        }

    }
}
