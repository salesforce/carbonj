/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMessage {
    @Test
    public void testMessageBuilder() {
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        Message message = new Message.Builder(payload).addHeader("foo", "bar").build();
        assertEquals("bar", message.getHeader("foo"));
        assertEquals("test", message.getHeader("invalid", "test"));
        assertEquals(payload, message.getPayload());
    }
}
