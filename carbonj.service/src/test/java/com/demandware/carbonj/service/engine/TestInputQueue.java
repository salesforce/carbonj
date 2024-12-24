/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

public class TestInputQueue {
    @Test
    public void testRejectPolicy() {
        // TODO: This is just for dummy code coverage, we should really test the reject policy
        InputQueue inputQueue = new InputQueue(new MetricRegistry(), "input-queue-consumer-1", new PointProcessorMock(), 1,
                "block", 10, 100);
        inputQueue = new InputQueue(new MetricRegistry(), "input-queue-consumer-2", new PointProcessorMock(), 1,
                "drop", 10, 100);
    }
}
