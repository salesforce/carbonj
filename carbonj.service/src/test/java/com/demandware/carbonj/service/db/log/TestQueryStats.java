/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestQueryStats {
    @Test
    public void testDefault() {
        QueryStats queryStats = new QueryStats();
        assertEquals(0, queryStats.getWaitTimeMillis().getCount());
        assertEquals(0, queryStats.getSeriesReadTimeMillis().getCount());
        assertEquals(0, queryStats.getSeriesWriteTimeMillis().getCount());
        assertEquals(0, queryStats.getEmptySeriesReadTimeMillis().getCount());
        assertEquals(0, queryStats.getTotalNoOfDataPoints());
    }
}
