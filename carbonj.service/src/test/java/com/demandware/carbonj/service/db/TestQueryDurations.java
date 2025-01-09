/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQueryDurations {
    @Test
    public void testQueryDurations() {
        QueryDurations queryDurations = new QueryDurations();
        queryDurations.addRead(1);
        assertEquals(1, queryDurations.read());
        queryDurations.addSerializeAndSend(2);
        assertEquals(2, queryDurations.serializeAndSend());
    }
}
