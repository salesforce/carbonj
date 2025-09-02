/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.nio.file.Path;

enum KinesisRecoveryProvider {
    FILESYSTEM,
    DYNAMODB
}

public class KinesisConfig {

    private final boolean kinesisConsumerEnabled;
    private final boolean recoveryEnabled;
    private final long recoveryIdleTimeMillis;
    private final long checkPointIntervalMillis;
    private final long retryTimeInMillis;
    private final int recoveryThreads;
    private final Path checkPointDir;
    private final int initRetryTimeInSecs;
    private final int leaseExpirationTimeInSecs;
    private final KinesisRecoveryProvider recoveryProvider;
    private final int gapsTableProvisionedThroughput;
    private final int maxRecords;
    private final boolean aggregationEnabled;

    public KinesisConfig(boolean kinesisConsumerEnabled, boolean recoveryEnabled,
                         long recoveryIdleTimeMillis, long checkPointIntervalMillis, long retryTimeInMillis,
                         int recoveryThreads, Path checkPointDir, int initRetryTimeInSecs,
                         int leaseExpirationTimeInSecs, String recoveryProvider, int gapsTableProvThroughput,
                         int maxRecords, boolean aggregationEnabled) {
        this.kinesisConsumerEnabled = kinesisConsumerEnabled;
        this.recoveryEnabled = recoveryEnabled;
        this.recoveryIdleTimeMillis = recoveryIdleTimeMillis;
        this.checkPointIntervalMillis = checkPointIntervalMillis;
        this.retryTimeInMillis = retryTimeInMillis;
        this.recoveryThreads = recoveryThreads;
        this.checkPointDir = checkPointDir;
        this.initRetryTimeInSecs = initRetryTimeInSecs;
        this.leaseExpirationTimeInSecs = leaseExpirationTimeInSecs;
        this.gapsTableProvisionedThroughput = gapsTableProvThroughput;
        this.maxRecords = maxRecords;
        this.aggregationEnabled = aggregationEnabled;

        if( recoveryProvider.equalsIgnoreCase("dynamodb")) {
            this.recoveryProvider = KinesisRecoveryProvider.DYNAMODB;
        } else {
            // default is filesystem
            this.recoveryProvider = KinesisRecoveryProvider.FILESYSTEM;
        }
    }

    public boolean isKinesisConsumerEnabled() {
        return kinesisConsumerEnabled;
    }

    public boolean isRecoveryEnabled() {
        return recoveryEnabled;
    }

    public long getRecoveryIdleTimeMillis() {
        return recoveryIdleTimeMillis;
    }

    public long getCheckPointIntervalMillis() {
        return checkPointIntervalMillis;
    }

    public long getRetryTimeInMillis() {
        return retryTimeInMillis;
    }

    public int recoveryThreads() {
        return recoveryThreads;
    }

    public Path getCheckPointDir() {
        return checkPointDir;
    }

    public int getInitRetryTimeInSecs() {
        return initRetryTimeInSecs;
    }

    public int getLeaseExpirationTimeInSecs() {
        return leaseExpirationTimeInSecs;
    }

    public KinesisRecoveryProvider getCheckPointProvider() { return recoveryProvider; }

    public int getGapsTableProvisionedThroughput() { return gapsTableProvisionedThroughput; }

    public int getMaxRecords() {
        return maxRecords;
    }

    public boolean isAggregationEnabled() {
        return aggregationEnabled;
    }
}
