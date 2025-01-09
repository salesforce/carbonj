/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestBlackListedQueryEvent {
    @Test
    public void test() throws Exception {
        BlackListedQueryEvent blackListedQueryEvent = new BlackListedQueryEvent("target", "from", "until", "now");
        assertEquals("target", blackListedQueryEvent.getTarget());
        assertEquals("from", blackListedQueryEvent.getFrom());
        assertEquals("until", blackListedQueryEvent.getUntil());
        assertEquals("now", blackListedQueryEvent.getNow());
        assertEquals("blacklisted", blackListedQueryEvent.getType());
        assertTrue(blackListedQueryEvent.getTime() <= System.currentTimeMillis());

        blackListedQueryEvent = new BlackListedQueryEvent("target");
        assertEquals("target", blackListedQueryEvent.getTarget());
        assertNull(blackListedQueryEvent.getFrom());
        assertNull(blackListedQueryEvent.getUntil());
        assertNull(blackListedQueryEvent.getNow());
        assertEquals("blacklisted", blackListedQueryEvent.getType());
        assertTrue(blackListedQueryEvent.getTime() <= System.currentTimeMillis());
    }
}
