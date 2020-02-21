/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;

import java.util.function.Consumer;

public interface Accumulator extends StatsAware {


    void add(DataPoint m);

    void rollUp(Consumer<DataPoints> out, long rollUpTimeInMillis, boolean force);

    int getMaxClosedSlotTs();

    SlotStrategy getSlotStrategy();

    void reset();
}
