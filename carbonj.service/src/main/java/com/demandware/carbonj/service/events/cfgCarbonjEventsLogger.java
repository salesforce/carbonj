/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { CfgKinesisEventsLogger.class } )
public class cfgCarbonjEventsLogger {

    @Value("${events.carbonj.batch.size:5}")
    private int batchSize;

    @Value("${events.carbonj.emptyQueuePauseMillis:2000}")
    private int emptyQueuePauseMillis;

    @Value("${events.carbonj.maxWaitTimeMillis:5000}")
    private int maxWaitTimeMillis;

    @Autowired
    MetricRegistry metricRegistry;

    @Bean(name = "CarbonjEventsLogger")
    public EventsLogger getEventsLogger(@Qualifier("KinesisEventsLogger") EventsLogger<byte[]> eventsLogger) throws Exception {
        return new CarbonJEventsLogger( metricRegistry, eventsLogger, batchSize, emptyQueuePauseMillis, maxWaitTimeMillis);
    }
}
