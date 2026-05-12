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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final int kinesisConsumerTracebackMinutes;

    public KinesisConsumer(MetricRegistry metricRegistry, PointProcessor pointProcessor, PointProcessor recoveryPointProcessor,
                           String kinesisStreamName, String kinesisApplicationName,
                           KinesisConfig kinesisConfig, CheckPointMgr<Date> checkPointMgr,
                           Counter noOfRestarts, String kinesisConsumerRegion, int kinesisConsumerTracebackMinutes) {
        this(metricRegistry, pointProcessor, recoveryPointProcessor, kinesisStreamName, kinesisApplicationName, kinesisConfig,
                checkPointMgr, noOfRestarts, kinesisConsumerRegion, kinesisConsumerTracebackMinutes, null);
    }

    public KinesisConsumer(MetricRegistry metricRegistry, PointProcessor pointProcessor, PointProcessor recoveryPointProcessor,
                           String kinesisStreamName, String kinesisApplicationName,
                           KinesisConfig kinesisConfig, CheckPointMgr<Date> checkPointMgr,
                           Counter noOfRestarts, String kinesisConsumerRegion,
                           int kinesisConsumerTracebackMinutes,
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
        this.kinesisConsumerTracebackMinutes = kinesisConsumerTracebackMinutes;
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
                // KCL only consults the initial position when no per-shard checkpoint exists. With traceback
                // disabled (default), start at the stream tip so the live consumer is never forced to drain
                // an outage backlog — the recovery path is responsible for filling gaps.
                InitialPositionInStreamExtended initialPositionInStreamExtended;
                if (kinesisConsumerTracebackMinutes <= 0) {
                    initialPositionInStreamExtended =
                            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
                } else {
                    Instant startTime = Instant.now().minus(Duration.ofMinutes(kinesisConsumerTracebackMinutes));
                    initialPositionInStreamExtended =
                            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(startTime.toEpochMilli()));
                }

                // KCL persists per-shard checkpoints in the lease table and resumes from them on
                // every restart, ignoring initialPositionInStreamExtended once leases exist. To
                // make traceback.minutes (and the default LATEST behavior) take effect on every
                // start, overwrite each lease's checkpoint to match the configured initial
                // position before starting the Scheduler. Skipped automatically when other
                // workers own leases — see seedLeaseCheckpoints().
                DynamoDbClientBuilder seedDdbBuilder = DynamoDbClient.builder()
                        .region(region).credentialsProvider(DefaultCredentialsProvider.builder().build());
                if (overrideKinesisEndpointUri != null) {
                    seedDdbBuilder.endpointOverride(overrideKinesisEndpointUri);
                }
                try (DynamoDbClient seedDdb = seedDdbBuilder.build()) {
                    seedLeaseCheckpoints(seedDdb, kinesisApplicationName, workerId,
                            kinesisConsumerTracebackMinutes, kinesisConfig.getResetLeasesOnStartup());
                }
                // KCL 3's load-aware LeaseAssignmentManager refuses to assign leases without
                // healthy WorkerMetricStats from a quorum of workers, which never happens for
                // single-worker deployments — the worker stays leader with leasesOwned=0
                // forever. Force the 2.x-style lease taker, which steals expired leases on a
                // fixed failoverTimeMillis window and matches carbonj's pre-migration behavior.
                CoordinatorConfig coordinatorConfig = configsBuilder.coordinatorConfig()
                        .clientVersionConfig(CoordinatorConfig.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X);
                worker = new Scheduler(
                        configsBuilder.checkpointConfig(),
                        coordinatorConfig,
                        leaseManagementConfig,
                        configsBuilder.lifecycleConfig(),
                        configsBuilder.metricsConfig(),
                        configsBuilder.processorConfig(),
                        configsBuilder.retrievalConfig()
                                .initialPositionInStreamExtended(initialPositionInStreamExtended)
                                .retrievalSpecificConfig(pollingConfig));
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

    /**
     * Overwrite every row's checkpoint in the KCL lease table so the next Scheduler start
     * uses the configured initialPositionInStreamExtended instead of the persisted checkpoint.
     * Skipped if the table doesn't exist (KCL will create it). In AUTO mode, skipped when
     * any row is owned by a worker other than this one — protects multi-worker deployments
     * from clobbering peers' progress.
     */
    private static void seedLeaseCheckpoints(DynamoDbClient ddb, String tableName, String workerId,
                                             int tracebackMinutes,
                                             KinesisConfig.ResetLeasesMode mode) {
        if (mode == KinesisConfig.ResetLeasesMode.NEVER) {
            return;
        }
        ScanResponse scan;
        try {
            scan = ddb.scan(ScanRequest.builder().tableName(tableName).build());
        } catch (ResourceNotFoundException e) {
            log.info("Lease table {} does not exist yet; KCL will create it.", tableName);
            return;
        }
        if (scan.items().isEmpty()) {
            log.info("Lease table {} is empty; nothing to seed.", tableName);
            return;
        }
        if (mode == KinesisConfig.ResetLeasesMode.AUTO) {
            for (Map<String, AttributeValue> item : scan.items()) {
                AttributeValue owner = item.get("leaseOwner");
                if (owner != null && owner.s() != null && !owner.s().equals(workerId)) {
                    log.warn("Lease table {} contains rows owned by other workers (e.g. {}). " +
                            "Skipping checkpoint reset to avoid clobbering their progress. " +
                            "Set kinesis.consumer.resetLeasesOnStartup=always to override.",
                            tableName, owner.s());
                    return;
                }
            }
        }
        String sentinel = tracebackMinutes <= 0 ? "LATEST" : "AT_TIMESTAMP";
        int updated = 0;
        for (Map<String, AttributeValue> item : scan.items()) {
            AttributeValue leaseKey = item.get("leaseKey");
            if (leaseKey == null || leaseKey.s() == null) {
                continue;
            }
            Map<String, AttributeValue> key = Collections.singletonMap("leaseKey", leaseKey);
            Map<String, AttributeValueUpdate> updates = new HashMap<>();
            updates.put("checkpoint", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(sentinel).build()).build());
            updates.put("checkpointSubSequenceNumber", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().n("0").build()).build());
            ddb.updateItem(b -> b.tableName(tableName).key(key).attributeUpdates(updates));
            updated++;
        }
        log.info("Seeded {} lease(s) in {} with checkpoint={}", updated, tableName, sentinel);
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
                kinesisConfig.getRecoveryGetRecordsLimit(), new GzipDataPointCodec());
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
