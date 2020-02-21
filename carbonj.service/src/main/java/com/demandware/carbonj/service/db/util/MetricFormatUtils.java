/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricFormatUtils
{

    public static String formatDBReadWriteTimerStats(Timer t)
    {
        Snapshot s = t.getSnapshot();
        return String.format("mean=%s, min=%s, max=%s, 95pt=%s, meanRate=%s, ma1=%s",
                        s.getMean(), s.getMin(), s.getMax(), s.get95thPercentile(), t.getMeanRate(), t.getOneMinuteRate());
    }
}
