/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;


import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

class PointProcessorTaskBuilder
{
    private final MetricRegistry metricRegistry;
    private final Consumer<DataPoints> out;
    private final MetricList blacklist;
    private final MetricList allowOnly;
    private final Relay auditLog;
    private final boolean aggregationEnabled;
    final PointFilter filter;
    private final Accumulator accumulator;
    private final NamespaceCounter nsCounter;

    private final boolean syncSecondaryDb;

    private final ConcurrentLinkedQueue<String> namespaceQueue;

    public PointProcessorTaskBuilder(MetricRegistry metricRegistry, Consumer<DataPoints> out,
                                     MetricList blacklist, MetricList allowOnly, Relay auditLog,
                                     boolean aggregationEnabled, PointFilter filter, Accumulator accumulator,
                                     NamespaceCounter nsCounter, boolean syncSecondaryDb,
                                     ConcurrentLinkedQueue<String> namespaceQueue) {
        this.metricRegistry = metricRegistry;
        this.out = out;
        this.blacklist = blacklist;
        this.allowOnly = allowOnly;
        this.auditLog = auditLog;
        this.aggregationEnabled = aggregationEnabled;
        this.filter = filter;
        this.accumulator = accumulator;
        this.nsCounter = Preconditions.checkNotNull(nsCounter);
        this.syncSecondaryDb = syncSecondaryDb;
        this.namespaceQueue = namespaceQueue;
    }

    public Runnable task(List<DataPoint> points)
    {
        return new PointProcessorTask(metricRegistry, points, blacklist, allowOnly, accumulator, aggregationEnabled, filter, out, auditLog, nsCounter, syncSecondaryDb, namespaceQueue);
    }

    public Accumulator getAccumulator() {
        return accumulator;
    }

    public Consumer<DataPoints> getOutProcessor() {
        return out;
    }
}
