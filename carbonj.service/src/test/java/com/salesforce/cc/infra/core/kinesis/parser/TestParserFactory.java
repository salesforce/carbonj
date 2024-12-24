/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

public class TestParserFactory {
    @Test
    public void testDefault() throws Exception {
        assertInstanceOf(DeprecatedParser.class, ParserFactory.getParser("0"));
        try {
            ParserFactory.getParser("2.0");
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            assertEquals("Unsupported version for parser: 2.0", e.getMessage());
        }
        byte[] bytes = "UMONTP/2.0".getBytes(StandardCharsets.UTF_8);
        try {
            ParserFactory.getParser(bytes);
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            assertEquals("Unsupported versionLine for protocol", e.getMessage());
        }
        bytes = "INVALID/1.0".getBytes(StandardCharsets.UTF_8);
        assertInstanceOf(DeprecatedParser.class, ParserFactory.getParser(bytes));
    }
}
