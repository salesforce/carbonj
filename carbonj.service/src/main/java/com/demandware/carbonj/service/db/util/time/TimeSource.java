/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util.time;

public interface TimeSource
{
    int getEpochSecond();

    TimeSource systemTimeSource = new SystemTimeSource();

    static TimeSource defaultTimeSource()
    {
        return systemTimeSource;
    }

    static TimeSource fixedTimeSource(int time)
    {
        return () -> time;
    }
}
