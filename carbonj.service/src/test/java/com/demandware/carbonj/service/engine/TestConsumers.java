/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class TestConsumers {

    @BeforeAll
    static void setUp() throws Exception {
        File dir = new File("./config");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        FileUtils.writeLines(new File(dir, "kinesis-test-stream-consumer.conf"), List.of("foo=bar"));
    }

    @Test
    public void testConsumers() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        File rulesFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("consumer-rules.conf")).getFile());
        KinesisConfig kinesisConfig = new KinesisConfig(true, false, 60000, 60000, 60000,
                1, Path.of("/tmp/checkpoint"), 60, 60, "recoveryProvider", 1, 1, 1000);
        FileCheckPointMgr checkPointMgr = new FileCheckPointMgr(Path.of("/tmp/checkpoint"), 5);
        Consumers consumers = new Consumers(metricRegistry, new PointProcessorMock(), new PointProcessorMock(),
                rulesFile, kinesisConfig, checkPointMgr, Region.US_EAST_1.id(), new NamespaceCounter(metricRegistry, 60), new File("/tmp/sync"));
        new KinesisRecordProcessorFactory(metricRegistry, new PointProcessorMock(), kinesisConfig, "test-stream");
        consumers.dumpStats();
        consumers.syncNamespaces();
        consumers.reload();
    }
}
