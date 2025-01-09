/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.demandware.carbonj.service.engine.DataPoint;

import java.util.concurrent.atomic.AtomicInteger;

public class CountingLatePointLogger implements LatePointLogger {

    private final AtomicInteger latePoints;

    public CountingLatePointLogger(AtomicInteger latePoints) {
        this.latePoints = latePoints;
    }

    @Override
    public void logLatePoint(DataPoint m, long now, Reason r, String context) {
        latePoints.incrementAndGet();
    }
}
