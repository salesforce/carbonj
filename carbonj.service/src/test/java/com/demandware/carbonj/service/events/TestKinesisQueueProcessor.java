/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

public class TestKinesisQueueProcessor {
    @Test
    public void test() {
        KinesisQueueProcessor kinesisQueueProcessor = new KinesisQueueProcessor(new MetricRegistry(), "test", null, 1);
        kinesisQueueProcessor.refreshStats();
        kinesisQueueProcessor.dumpStats();
    }
}
