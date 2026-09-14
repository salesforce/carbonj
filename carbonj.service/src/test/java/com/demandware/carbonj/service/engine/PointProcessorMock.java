/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.accumulator.Accumulator;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PointProcessorMock implements PointProcessor {
    private final AtomicInteger counter = new AtomicInteger();
    private final Set<String> metricNames = ConcurrentHashMap.newKeySet();

    @Override
    public void process(List<DataPoint> points) {
        counter.addAndGet(points.size());
        points.forEach(point -> metricNames.add(point.name));
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
        return counter.get();
    }

    public Set<String> getMetricNames() {
        return Set.copyOf(metricNames);
    }
}
