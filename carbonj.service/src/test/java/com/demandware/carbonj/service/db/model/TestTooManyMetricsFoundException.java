/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTooManyMetricsFoundException {
    @Test
    public void test() {
        TooManyMetricsFoundException exception1 = new TooManyMetricsFoundException(1);
        assertEquals(1, exception1.getLimit());
        TooManyMetricsFoundException exception2 = new TooManyMetricsFoundException(2, "error");
        assertEquals(2, exception2.getLimit());
        assertEquals("error", exception2.getMessage());
    }
}
