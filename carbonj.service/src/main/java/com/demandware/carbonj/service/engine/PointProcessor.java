/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.db.util.StatsAware;

import java.util.List;

public interface PointProcessor extends StatsAware {

    void process(List<DataPoint> points);

    void drain();

    void close();

    Accumulator getAccumulator();

    void flushAggregations(boolean force);
}
