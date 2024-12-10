/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;

import java.util.Date;

public class KinesisConsumer extends Thread {

    private static final Logger log = LoggerFactory.getLogger(KinesisConsumer.class);

    private final String kinesisStreamName;

    private final String kinesisApplicationName;

    private final ConfigsBuilder configsBuilder;
    private Scheduler scheduler;
    private final Counter noOfRestarts;

    private volatile boolean closed;

    public KinesisConsumer(String kinesisStreamName, String kinesisApplicationName,
                           Counter noOfRestarts, ConfigsBuilder configsBuilder) {
        this.kinesisStreamName = kinesisStreamName;
        this.kinesisApplicationName = kinesisApplicationName;
        this.noOfRestarts = noOfRestarts;
        this.configsBuilder = configsBuilder;
        this.scheduler = new Scheduler(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(), configsBuilder.lifecycleConfig(), configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(), configsBuilder.retrievalConfig());
        log.info("Kinesis consumer {} with name {} started", kinesisStreamName, kinesisApplicationName);
        this.start();
    }

    public void run () {

        while (!closed) {
            scheduler.run();
            while (!scheduler.shutdownComplete()) {
                log.info("Kinesis consumer {} with name {} shutting down", kinesisStreamName, kinesisApplicationName);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            this.scheduler = new Scheduler(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
                    configsBuilder.leaseManagementConfig(), configsBuilder.lifecycleConfig(), configsBuilder.metricsConfig(),
                    configsBuilder.processorConfig(), configsBuilder.retrievalConfig());
            noOfRestarts.inc();
        }
    }

    void closeQuietly() {
        closed = true;
        if (scheduler != null) {
            scheduler.shutdown();
        }
        log.info(String.format("Kinesis stream %s consumer stopped", kinesisStreamName));
    }

    public void dumpStats() {
        log.info( String.format( "Metrics consumed in kinesis stream %s =%s", kinesisStreamName,
                KinesisRecordProcessorFactory.metricsReceived.getCount() ));
        log.info( String.format( "Messages consumed in kinesis stream %s = %s",  kinesisStreamName,
                KinesisRecordProcessorFactory.messagesReceived.getCount() ));
    }
}
