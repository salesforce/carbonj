/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CfgKinesisEventsLogger {

    @Value("${events.kinesis.stream:umon-prd-logging}")
    String streamName;

    @Value("${events.kinesis.queue.size:50000}")
    int queueSize;

    @Value("${events.kinesis.batch.size:25}")
    int batchSize;

    @Value("${events.kinesis.emptyQueuePauseMillis:200}")
    private int emptyQueuePauseMillis;

    @Value("${events.kinesis.maxWaitTimeMillis:10000}")
    private int maxWaitTimeMillis;

    @Value("${events.kinesis.threads:3}")
    private int noOfThreads;

    @Value( "${events.kinesis.rbacEnabled:false}" )
    private Boolean rbacEnabled = false;

    @Value( "${events.kinesis.region:us-east-1}" )
    private String region = "us-east-1";

    @Value( "${events.kinesis.account:}" )
    private String account;

    @Value( "${events.kinesis.role:}" )
    private String role;

    @Autowired
    MetricRegistry metricRegistry;

    @Bean(name = "KinesisEventsLogger")
    public EventsLogger getKinesisEventsLogger()  {
        return new KinesisEventsLogger(metricRegistry, streamName, rbacEnabled, region, account, role, queueSize, emptyQueuePauseMillis, noOfThreads, batchSize, maxWaitTimeMillis);
    }
}
