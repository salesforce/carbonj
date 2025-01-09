/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSystemTime {
    @Test
    public void testSystemTime() {
        Clock clock = Clock.systemUTC();
        SystemTime.setClockForTest(clock);
        assertEquals(clock.millis() / 1000, SystemTime.nowEpochSecond());
    }
}
