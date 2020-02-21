/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public class EventsHandlerImpl implements EventsHandler {

    private final EventsLogger<byte[]> eventsLogger;

    EventsHandlerImpl(EventsLogger<byte[]> eventsLogger) {
        this.eventsLogger = eventsLogger;
    }

    @Override
    public void process(ByteBuf buff) {
        byte[] eventBytes = buff.toString( io.netty.util.CharsetUtil.US_ASCII ).getBytes(StandardCharsets.US_ASCII);
        eventsLogger.log(eventBytes);
    }
}
