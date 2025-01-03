/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.accumulator.Accumulator;

import java.util.List;

public class PointProcessorMock implements PointProcessor {
    private int counter = 0;

    @Override
    public void process(List<DataPoint> points) {
        counter += points.size();
    }

    @Override
    public void drain() {

    }

    @Override
    public void close() {

    }

    @Override
    public Accumulator getAccumulator() {
        return null;
    }

    @Override
    public void flushAggregations(boolean force) {

    }

    @Override
    public void dumpStats() {
    }

    public int getCounter() {
        return counter;
    }
}
