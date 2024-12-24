/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;

import com.salesforce.cc.infra.core.kinesis.Message;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestDeprecatedParser {
    @Test
    public void testDefault() {
        DeprecatedParser parser = new DeprecatedParser();
        try {
            parser.encode(null);
            fail("Should have thrown an exception");
        } catch (UnsupportedOperationException e) {
            assertEquals("Deprecated", e.getMessage());
        }
        byte[] bytes = "test".getBytes(StandardCharsets.UTF_8);
        Message message = parser.decode(bytes);
        assertTrue(message.getHeaders().isEmpty());
        assertEquals(bytes, message.getPayload());
    }
}
