/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.demandware.carbonj.service.engine.DataPoint;

public interface LatePointLogger {
    void logLatePoint(DataPoint m, long now, Reason r, String context);

    enum Reason
    {
        SLOT_CLOSED,
        SLOT_EXPIRED
    }
}
