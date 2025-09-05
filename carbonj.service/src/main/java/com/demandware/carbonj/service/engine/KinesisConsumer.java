/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.engine.recovery.DynamoDbGapsTableImpl;
import com.demandware.carbonj.service.engine.recovery.FileSystemGapsTableImpl;
import com.demandware.carbonj.service.engine.recovery.Gap;
import com.demandware.carbonj.service.engine.recovery.GapImpl;
import com.demandware.carbonj.service.engine.recovery.GapsTable;
import com.demandware.carbonj.service.engine.recovery.RecoveryManager;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KinesisConsumer extends Thread {

    private static final Logger log = LoggerFactory.getLogger(KinesisConsumer.class);

    private final MetricRegistry metricRegistry;

    private final PointProcessor pointProcessor;

    private final String kinesisStreamName;

    private final String kinesisApplicationName;

    private final KinesisConfig kinesisConfig;
    private final CheckPointMgr<Date> checkPointMgr;
    private final Counter noOfRestarts;

    private Scheduler worker;

    private final PointProcessor recoveryPointProcessor;

    private volatile boolean closed;

    private final String kinesisConsumerRegion;

    private final String overrideKinesisEndpoint;

    public KinesisConsumer(MetricRegistry metricRegistry, PointProcessor pointProcessor, PointProcessor recoveryPointProcessor,
                           String kinesisStreamName, String kinesisApplicationName,
                           KinesisConfig kinesisConfig, CheckPointMgr<Date> checkPointMgr,
                           Counter noOfRestarts, String kinesisConsumerRegion) {
        this(metricRegistry, pointProcessor, recoveryPointProcessor, kinesisStreamName, kinesisApplicationName, kinesisConfig,
                checkPointMgr, noOfRestarts, kinesisConsumerRegion, null);
    }

    public KinesisConsumer(MetricRegistry metricRegistry, PointProcessor pointProcessor, PointProcessor recoveryPointProcessor,
                           String kinesisStreamName, String kinesisApplicationName,
                           KinesisConfig kinesisConfig, CheckPointMgr<Date> checkPointMgr,
                           Counter noOfRestarts, String kinesisConsumerRegion,
                           String overrideKinesisEndpoint) {
        this.metricRegistry = metricRegistry;
        this.pointProcessor = Preconditions.checkNotNull(pointProcessor);
        this.recoveryPointProcessor = recoveryPointProcessor;
        this.kinesisStreamName = kinesisStreamName;
        this.kinesisApplicationName = kinesisApplicationName;
        this.kinesisConfig = kinesisConfig;
        this.checkPointMgr = checkPointMgr;
        this.noOfRestarts = noOfRestarts;
        this.kinesisConsumerRegion = kinesisConsumerRegion;
        this.overrideKinesisEndpoint = overrideKinesisEndpoint;
        log.info("Kinesis consumer started");
        this.start();
    }

    public void run () {

        while (!closed) {
            try {
                String workerId = kinesisApplicationName + "-worker";

                URI overrideKinesisEndpointUri = null;
                if (overrideKinesisEndpoint != null && !StringUtils.isEmpty(overrideKinesisEndpoint.trim())) {
                    overrideKinesisEndpointUri = java.net.URI.create(overrideKinesisEndpoint);
                }
                if (overrideKinesisEndpointUri != null) {
                    log.info("Overridden Kinesis endpoint = {}", overrideKinesisEndpointUri);
                }

                if (kinesisConfig.isRecoveryEnabled()) {
                    initCatchupKinesisClient(overrideKinesisEndpointUri);
                }

                Region region = Region.of(kinesisConsumerRegion);
                KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient.builder()
                        .region(region).credentialsProvider(DefaultCredentialsProvider.builder().build());
                if (overrideKinesisEndpointUri != null) {
                    kinesisAsyncClientBuilder.endpointOverride(overrideKinesisEndpointUri);
                }
                KinesisAsyncClient kinesisAsync = kinesisAsyncClientBuilder.build();
                DynamoDbAsyncClientBuilder dynamoDbAsyncClientBuilder = DynamoDbAsyncClient.builder()
                        .region(region).credentialsProvider(DefaultCredentialsProvider.builder().build());
                if (overrideKinesisEndpointUri != null) {
                    dynamoDbAsyncClientBuilder.endpointOverride(overrideKinesisEndpointUri);
                }
                DynamoDbAsyncClient dynamoAsync = dynamoDbAsyncClientBuilder.build();
                CloudWatchAsyncClientBuilder cloudWatchAsyncClientBuilder = CloudWatchAsyncClient.builder()
                        .region(region).credentialsProvider(DefaultCredentialsProvider.builder().build());
                if (overrideKinesisEndpointUri != null) {
                    cloudWatchAsyncClientBuilder.endpointOverride(overrideKinesisEndpointUri);
                }
                CloudWatchAsyncClient cloudWatchAsync = cloudWatchAsyncClientBuilder.build();

                ShardRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(metricRegistry, pointProcessor,
                        kinesisConfig, kinesisStreamName, checkPointMgr);

                ConfigsBuilder configsBuilder = new ConfigsBuilder(
                        kinesisStreamName,
                        kinesisApplicationName,
                        kinesisAsync,
                        dynamoAsync,
                        cloudWatchAsync,
                        workerId,
                        recordProcessorFactory);

                // Configure retrieval with polling and set initial position
                PollingConfig pollingConfig = new PollingConfig(kinesisStreamName, kinesisAsync);
                if (kinesisConfig.getMaxRecords() > 0) {
                    pollingConfig.maxRecords(kinesisConfig.getMaxRecords());
                }
                // Map v1 withFailoverTimeMillis -> v2/3 failoverTimeMillis
                LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig()
                        .failoverTimeMillis(kinesisConfig.getLeaseExpirationTimeInSecs() * 1000L);
                worker = new Scheduler(
                        configsBuilder.checkpointConfig(),
                        configsBuilder.coordinatorConfig(),
                        leaseManagementConfig,
                        configsBuilder.lifecycleConfig(),
                        configsBuilder.metricsConfig(),
                        configsBuilder.processorConfig(),
                        configsBuilder.retrievalConfig()
                                .initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST))
                                .retrievalSpecificConfig(pollingConfig)
                );
                log.info("KCL v2 Scheduler started with app {}, stream {}, workerId {}", kinesisApplicationName, kinesisStreamName, workerId);
                worker.run();
            } catch (Throwable t) {
                log.error("Error in initializing kinesis consumer", t);

                shutdownQuietly(worker);

                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(kinesisConfig.getInitRetryTimeInSecs()));
                } catch (InterruptedException e) {
                    log.error("wait interrupted", e);
                }

                noOfRestarts.inc();
            }
        }
    }

    private void shutdownQuietly(Scheduler worker) {
        try {
            if (worker != null) {
                worker.shutdown();
            }
        } catch (Throwable throwable) {
            log.error("worker shutdown failed!", throwable);
        }
    }

    private void initCatchupKinesisClient(URI overrideKinesisEndpointUri) throws Exception {
        log.info("Initializing kinesis recovery processing..");
        GapsTable gapsTable;
        if( kinesisConfig.getCheckPointProvider() == KinesisRecoveryProvider.DYNAMODB ) {
            DynamoDbClientBuilder dynamoDbClientBuilder = DynamoDbClient.builder().region(Region.of(kinesisConsumerRegion));
            if (overrideKinesisEndpointUri != null) {
                dynamoDbClientBuilder.endpointOverride(overrideKinesisEndpointUri);
            }
            gapsTable = new DynamoDbGapsTableImpl(dynamoDbClientBuilder.build(), kinesisApplicationName, kinesisConfig.getGapsTableProvisionedThroughput());
        } else {
            gapsTable = new FileSystemGapsTableImpl(kinesisConfig.getCheckPointDir());
        }

        KinesisClientBuilder kinesisClientBuilder = KinesisClient.builder().region(Region.of(kinesisConsumerRegion));
        if (overrideKinesisEndpointUri != null) {
            kinesisClientBuilder.endpointOverride(overrideKinesisEndpointUri);
        }
        KinesisClient kinesisClient = kinesisClientBuilder.build();

        long gapStartTimeInMillis = checkPointMgr.lastCheckPoint().getTime();
        long gapEndTimeInMillis = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);

        // if the carbonj had restarted before any new checkpoint is committed, avoid overlapping gaps.
        List<Gap> gaps = gapsTable.getGaps();
        int noOfGaps = gaps.size();
        if (noOfGaps > 0) {
            Gap lastGap = gaps.get(noOfGaps - 1);
            long lastGapEndTimeInMillis = lastGap.endTime().getTime();
            if (lastGapEndTimeInMillis > gapStartTimeInMillis) {
                gapStartTimeInMillis = lastGapEndTimeInMillis;
            }
        }

        gapsTable.add(new GapImpl(new Date(gapStartTimeInMillis), new Date(gapEndTimeInMillis)));

        RecoveryManager recoveryManager = new RecoveryManager(metricRegistry, gapsTable, kinesisStreamName, recoveryPointProcessor, kinesisClient,
                kinesisConfig.getRecoveryIdleTimeMillis(), kinesisConfig.getRetryTimeInMillis(),
                new GzipDataPointCodec());
        new Thread(recoveryManager).start();
    }

    void closeQuietly() {
        closed = true;
        shutdownQuietly(worker);
        log.info("Kinesis stream {} consumer stopped", kinesisStreamName);
    }

    public void dumpStats() {
        log.info("Metrics consumed in kinesis stream {} = {}", kinesisStreamName, KinesisRecordProcessorFactory.metricsReceived.getCount());
        log.info("Messages consumed in kinesis stream {} = {}", kinesisStreamName, KinesisRecordProcessorFactory.messagesReceived.getCount());
    }
}
