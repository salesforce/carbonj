/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import java.time.Clock;

public class SystemTime
{
    private static Clock clock = Clock.systemUTC();

    public static int nowEpochSecond()
    {
        return Math.toIntExact( clock.instant().getEpochSecond() );
    }

    /**
     * Should only be used for testing.
     *
     * @param testClock
     * @return clock instance that was replaced.
     */
    public static Clock setClockForTest(Clock testClock)
    {
        Clock original = clock;
        clock = testClock;
        return original;
    }
}

