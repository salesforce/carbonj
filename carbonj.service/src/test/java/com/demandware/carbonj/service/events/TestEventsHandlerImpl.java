/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class TestEventsHandlerImpl {
    @Test
    public void test() {
        EventsHandlerImpl eventsHandler = new EventsHandlerImpl(new NoOpLogger<>());
        eventsHandler.process(Unpooled.wrappedBuffer("test".getBytes(StandardCharsets.UTF_8)));
    }
}
