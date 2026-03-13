/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;

@Configuration
public class cfgKinesis
{

    @Value( "${kinesis.recovery.idleTimeMilliSeconds:1000}" )
    private int recoveryIdleTimeInMillis;

    @Value( "${kinesis.recovery.retryTimeInMillis:1000}" )
    private int retryTimeInMillis;

    @Value( "${kinesis.recoveryThreads:1}" )
    private int recoveryThreads;

    @Value( "${kinesis.consumer.enabled:false}" )
    private boolean kinesisConsumerEnabled;

    @Value( "${kinesis.consumer.maxRecords:10}" )
    private int maxRecords;

    @Value( "${kinesis.checkPoint.interval.millis:60000}" )
    private long checkPointIntervalMillis;

    @Value( "${kinesis.recovery.enabled:true}" )
    private boolean recoveryEnabled;

    @Value( "${metrics.store.checkPoint.dir:work/carbonj-checkpoint}")
    private String checkPointDir;

    @Value( "${kinesis.consumer.initRetryTimeInSecs:5}" )
    private int initRetryTimeInSecs;

    @Value( "${kinesis.consumer.leaseExpirationTimeInSecs:900}" )
    private int leaseExpirationTimeInSecs;

    @Value( "${kinesis.recovery.provider:filesystem}" )
    private String recoveryProvider;

    @Value( "${kinesis.recovery.gapsTable.provisionedThroughput:2}" )
    private int gapsTableProvisionedThroughput;

    @Value( "${aggregation.enabled:true}" )
    private boolean aggregationEnabled;

    // Optional: dynamically suffix the KCL application name and cleanup old lease tables
    @Value( "${kinesis.consumer.appName.dynamicSuffix.enabled:false}" )
    private boolean appNameDynamicSuffixEnabled;

    // Supports tokens: {epoch}, {uuid}, {hostname}
    @Value( "${kinesis.consumer.appName.dynamicSuffix.format:-{epoch}}" )
    private String appNameDynamicSuffixFormat;

    @Value( "${kinesis.consumer.cleanupOldLeaseTables.enabled:false}" )
    private boolean cleanupOldLeaseTablesEnabled;

    @Bean
    KinesisConfig kinesisConfig()
    {
        return new KinesisConfig(kinesisConsumerEnabled, recoveryEnabled, recoveryIdleTimeInMillis,
                checkPointIntervalMillis, retryTimeInMillis, recoveryThreads, Paths.get(checkPointDir),
                initRetryTimeInSecs, leaseExpirationTimeInSecs, recoveryProvider, gapsTableProvisionedThroughput,
                maxRecords, aggregationEnabled,
                appNameDynamicSuffixEnabled, appNameDynamicSuffixFormat, cleanupOldLeaseTablesEnabled);
    }
}
