/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.reporter;

import com.codahale.metrics.*;
import com.sfcc.um.metrics_reporter.transport.GraphiteTransport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGraphiteReporter {
    @Test
    public void testGraphiteReporter() {
        MetricRegistry registry = new MetricRegistry();
        GraphiteReporter.Builder builder = GraphiteReporter.forRegistry(registry);
        TestGraphiteTransport graphiteTransport = new TestGraphiteTransport();
        try (GraphiteReporter graphiteReporter = builder
                .withPrefix("prefix")
                .withClock(Clock.defaultClock())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphiteTransport)) {
            SortedMap<String, Gauge> gauges = new TreeMap<>();
            gauges.put("gauge", (Gauge<Double>) () -> 1.0);
            SortedMap<String, Counter> counters = new TreeMap<>();
            counters.put("counter", new Counter());
            SortedMap<String, Histogram> histograms = new TreeMap<>();
            histograms.put("histogram", new Histogram(new SlidingWindowReservoir(1)));
            SortedMap<String, Meter> meters = new TreeMap<>();
            meters.put("meter", new Meter());
            SortedMap<String, Timer> timers = new TreeMap<>();
            timers.put("timer", new Timer());
            graphiteReporter.report(gauges, counters, histograms, meters, timers);
        }
        assertEquals(99, graphiteTransport.data.size());
    }

    private static class TestGraphiteTransport implements GraphiteTransport {
        private final List<String> data = new ArrayList<>();

        @Override
        public GraphiteTransport open() {
            return null;
        }

        @Override
        public void send(String name, String value, long timestamp) {
            data.add(name + " " + value + " " + timestamp);
        }

        @Override
        public void close() {
        }
    }
}
