/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestFileDestination {
    private static final String fileName = "/tmp/file_destination_test";

    @Test
    public void test() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        DataPoint dataPoint = new DataPoint("foo.bar", 123, (int) (System.currentTimeMillis() / 1000));
        FileDestination fileDestination = new FileDestination(metricRegistry, "audit", fileName, 1);
        assertEquals(fileName, fileDestination.toString());
        assertEquals("dest.audit._tmp_file_destination_test.queue", fileDestination.queueSizeGaugeName());
        fileDestination.accept(dataPoint);
        Thread.sleep(500);
        fileDestination.closeQuietly();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ");
                assertEquals(dataPoint.name, parts[0]);
                assertEquals(dataPoint.val, Double.valueOf(parts[1]));
                assertEquals(dataPoint.ts, Integer.valueOf(parts[2]));
            }
        }
        checkMeter(metricRegistry, fileDestination.name + ".sent", 1L);
    }

    private void checkMeter(MetricRegistry metricRegistry, String name, long expected) {
        Meter meter = metricRegistry.getMeters().get(name);
        assertNotNull(meter);
        assertEquals(expected, meter.getCount());
    }
}
