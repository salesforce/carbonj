/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.LinkedList;

public class TestCarbonjEventsQueueProcessor {
    @Test
    public void test() {
        CarbonjEventsQueueProcessor carbonjEventsQueueProcessor = new CarbonjEventsQueueProcessor(new MetricRegistry(), new NoOpLogger<>());
        Collection<JsonObject> jsonObjectCollection = new LinkedList<>();
        jsonObjectCollection.add(new JsonObject());
        carbonjEventsQueueProcessor.process(jsonObjectCollection);
    }
}
