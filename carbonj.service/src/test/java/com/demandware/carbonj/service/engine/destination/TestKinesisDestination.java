/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

public class TestKinesisDestination {

    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Test
    public void testDestination() {
        KinesisDestination kinesisDestination = new KinesisDestination(metricRegistry, "test", 1,
                "test-stream", 1, 1, 10, "us-east-1",
                false, "123456", "test-role");

        kinesisDestination.accept((new DataPoint("metric1", 1, (int) System.currentTimeMillis()/1000)));
        kinesisDestination.close();
    }

    @Test
    public void testRbac() {
        KinesisDestination kinesisDestination = new KinesisDestination(metricRegistry, "test", 1,
                "test-stream", 1, 1, 10, "us-east-1",
                true, "123456", "test-role");

        kinesisDestination.accept((new DataPoint("metric1", 1, (int) System.currentTimeMillis()/1000)));
        kinesisDestination.close();
    }
}
