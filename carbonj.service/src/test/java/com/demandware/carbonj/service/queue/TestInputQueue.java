/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.queue;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.events.DropRejectionHandler;
import com.demandware.carbonj.service.events.TestCarbonjEventsLogger;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestInputQueue {
    @Test
    public void testDefault() {
        MetricRegistry metricRegistry  = new MetricRegistry();
        InputQueue inputQueue = new InputQueue(metricRegistry, "test", new TestCarbonjEventsLogger.MockQueueProcessor(),
                1, new DropRejectionHandler<JsonObject>(), 1, 1000, 1000);
        inputQueue.accept(new JsonObject());
        assertEquals(1, inputQueue.queuedItemsCount());
        inputQueue.accept(new JsonObject());
        assertEquals(1, inputQueue.queuedItemsCount());
        inputQueue.refreshStats();
        inputQueue.dumpStats();
    }
}
