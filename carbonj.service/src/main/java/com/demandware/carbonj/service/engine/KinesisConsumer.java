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
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

        // KCL expects a unique workerId per process so that lease ownership and expiry
        // semantics work correctly. Hostname makes it readable in logs / lease tables;
        // the UUID guarantees uniqueness even when hostnames collide (e.g. multiple
        // restarts of the same pod within a failover window).
        final String workerId = resolveHostId() + ":" + UUID.randomUUID();

        // Anchor the live consumer's initial position to a fixed wall-clock instant
        // captured at startup. KCL's LATEST iterator only returns records that arrive
        // *after* the iterator is created, which can leave a gap of seconds-to-minutes
        // between recovery's gapEndTime (startup + 2min) and KCL's first
        // GetShardIterator call. AT_TIMESTAMP filters by record arrival time on the
        // Kinesis side, so the live consumer always reads from this anchor regardless
        // of when KCL gets around to creating the iterator. The traceback property
        // optionally extends this anchor further into the past for fresh deployments
        // that want the live path to backfill instead of relying on recovery.
        final Instant liveStartInstant = Instant.now()
                .minus(Duration.ofMinutes(Math.max(0, kinesisConsumerTracebackMinutes)));
        boolean firstIteration = true;

        while (!closed) {
            try {
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
                InitialPositionInStreamExtended initialPositionInStreamExtended =
                        InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
                                new Date(liveStartInstant.toEpochMilli()));

                // KCL persists per-shard checkpoints in the lease table and resumes from them on
                // every restart, ignoring initialPositionInStreamExtended once leases exist. To
                // make the AT_TIMESTAMP(liveStartInstant) anchor take effect on every start,
                // overwrite each lease's checkpoint before starting the Scheduler.
                //
                // Only seed on the first iteration of the retry loop. A transient KCL exception
                // re-enters this loop; running the seed again would clobber valid in-flight
                // checkpoints just before recovery would otherwise resume from them. This means
                // the in-process retry loop only recovers from transient failures — if the seed
                // or forceCoordinatorStateToV2Compat partially completed and left the DDB state
                // inconsistent, only a process restart will re-run them.
                if (firstIteration) {
                    DynamoDbClientBuilder seedDdbBuilder = DynamoDbClient.builder()
                            .region(region).credentialsProvider(DefaultCredentialsProvider.builder().build());
                    if (overrideKinesisEndpointUri != null) {
                        seedDdbBuilder.endpointOverride(overrideKinesisEndpointUri);
                    }
                    try (DynamoDbClient seedDdb = seedDdbBuilder.build()) {
                        seedLeaseCheckpoints(seedDdb, kinesisApplicationName,
                                kinesisConfig.getResetLeasesOnStartup());
                        forceCoordinatorStateToV2Compat(seedDdb, kinesisApplicationName, workerId);
                    }
                    firstIteration = false;
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
     * Resolve the local hostname for use as the human-readable prefix of the KCL
     * workerId. This is purely for diagnostics — uniqueness is guaranteed by a UUID
     * appended after this value. On Kubernetes this is typically the pod name.
     */
    private static String resolveHostId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Unable to resolve local hostname; using 'unknown' for workerId prefix.", e);
            return "unknown";
        }
    }

    /**
     * Overwrite every row's checkpoint in the KCL lease table so the next Scheduler start
     * uses the configured initialPositionInStreamExtended instead of the persisted checkpoint.
     * Skipped if the table doesn't exist (KCL will create it).
     *
     * AUTO and ALWAYS both reset every lease unconditionally. We deliberately do not try
     * to detect peer workers via leaseOwner: workerId is unique per process (hostname +
     * UUID), so any previous run's leases — whether from this same pod or a redeployed
     * pod with a different hostname — will look like a peer's. We have no reliable way
     * to distinguish "my own previous run" from "a live peer in a multi-worker fleet"
     * without external state.
     *
     * For multi-worker deployments where multiple pods share a kinesisApplicationName,
     * operators must set kinesis.consumer.resetLeasesOnStartup=never so the live
     * consumers can resume from their persisted checkpoints without being clobbered by
     * any peer's startup. The default (single-worker, reset on startup) matches the v1
     * KCL MemLeaseManager behavior carbonj relied on for years.
     */
    private static void seedLeaseCheckpoints(DynamoDbClient ddb, String tableName,
                                             KinesisConfig.ResetLeasesMode mode) {
        if (mode == KinesisConfig.ResetLeasesMode.NEVER) {
            return;
        }
        List<Map<String, AttributeValue>> items;
        try {
            items = scanAllLeases(ddb, tableName);
        } catch (ResourceNotFoundException e) {
            log.info("Lease table {} does not exist yet; KCL will create it.", tableName);
            return;
        }
        if (items.isEmpty()) {
            log.info("Lease table {} is empty; nothing to seed.", tableName);
            return;
        }
        // The live consumer's initial position is always AT_TIMESTAMP (configured per
        // run), so the lease checkpoint sentinel must match — KCL reads the timestamp
        // from initialPositionInStreamExtended whenever it sees this sentinel.
        int updated = 0;
        for (Map<String, AttributeValue> item : items) {
            AttributeValue leaseKey = item.get("leaseKey");
            if (leaseKey == null || leaseKey.s() == null) {
                continue;
            }
            // Surface the previous owner so multi-worker deployments that forgot to
            // set resetLeasesOnStartup=never can spot the misconfiguration in logs.
            AttributeValue priorOwner = item.get("leaseOwner");
            if (priorOwner != null && priorOwner.s() != null) {
                log.info("Resetting lease {} previously owned by {} to checkpoint=AT_TIMESTAMP",
                        leaseKey.s(), priorOwner.s());
            }
            Map<String, AttributeValue> key = Collections.singletonMap("leaseKey", leaseKey);
            Map<String, AttributeValueUpdate> updates = new HashMap<>();
            updates.put("checkpoint", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s("AT_TIMESTAMP").build()).build());
            updates.put("checkpointSubSequenceNumber", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().n("0").build()).build());
            // Drop attributes that KCL also reads when reconstructing a Lease. A stale
            // pendingCheckpoint left from a previous run's mid-prepareCheckpoint crash is
            // not a sentinel and would route IteratorBuilder to AT_SEQUENCE_NUMBER, undoing
            // the AT_TIMESTAMP we just wrote. checkpointOwner is similar — stale values
            // break getDynamoLeaseOwnerExpectation conditional updates on lease takeover.
            updates.put("pendingCheckpoint", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
            updates.put("pendingCheckpointSubSequenceNumber", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
            updates.put("pendingCheckpointState", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
            updates.put("checkpointOwner", AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());
            ddb.updateItem(b -> b.tableName(tableName).key(key).attributeUpdates(updates));
            updated++;
        }
        log.info("Seeded {} lease(s) in {} with checkpoint=AT_TIMESTAMP", updated, tableName);
    }

    /**
     * Force the KCL CoordinatorState row to CLIENT_VERSION_2X so KCL takes the 2x-compat
     * lease-assignment path, regardless of whatever was persisted by an earlier build.
     *
     * Without this, KCL's MigrationStateMachine reads the persistent {appName}-CoordinatorState
     * row at startup and overrides our application's CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X
     * config if the row says CLIENT_VERSION_3X. The 3x path uses load-aware lease
     * assignment which won't assign leases until the LeaseAssignmentManager has accumulated
     * sufficient WorkerMetricStats samples — a process that takes ~10 minutes — during
     * which the live consumer reads no records. Writing CLIENT_VERSION_2X here makes
     * KCL go straight to the 2x-style lease taker.
     *
     * Schema (per software.amazon.kinesis.coordinator.MigrationState):
     *   key (S) = "Migration3.0"   — hash key
     *   cv (S)  = client version sentinel
     *   mts (N) = modified timestamp millis
     *   mb (S)  = modified-by worker id
     *   h (L)   = history list (left untouched; informational)
     */
    private static void forceCoordinatorStateToV2Compat(DynamoDbClient ddb, String appName, String workerId) {
        String tableName = appName + "-CoordinatorState";
        Map<String, AttributeValue> key = Collections.singletonMap("key",
                AttributeValue.builder().s("Migration3.0").build());
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("cv", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("CLIENT_VERSION_2X").build()).build());
        updates.put("mts", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().n(Long.toString(System.currentTimeMillis())).build()).build());
        updates.put("mb", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s(workerId).build()).build());
        try {
            ddb.updateItem(b -> b.tableName(tableName).key(key).attributeUpdates(updates));
            log.info("Forced {} Migration3.0 to CLIENT_VERSION_2X", tableName);
        } catch (ResourceNotFoundException e) {
            // Table doesn't exist yet (first ever start). KCL will create it and
            // initialise from our CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X config,
            // which writes CLIENT_VERSION_2X — so no action needed here.
            log.info("CoordinatorState table {} does not exist yet; KCL will create it.", tableName);
        }
    }

    /**
     * DynamoDB Scan returns at most 1 MB per call. For lease tables with many shards
     * a single call would miss leases beyond the first page, defeating both the AUTO
     * ownership check and the seed update for those leases.
     */
    private static List<Map<String, AttributeValue>> scanAllLeases(DynamoDbClient ddb, String tableName) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> lastKey = null;
        do {
            ScanRequest.Builder req = ScanRequest.builder().tableName(tableName);
            if (lastKey != null) {
                req.exclusiveStartKey(lastKey);
            }
            ScanResponse resp = ddb.scan(req.build());
            items.addAll(resp.items());
            lastKey = resp.hasLastEvaluatedKey() ? resp.lastEvaluatedKey() : null;
        } while (lastKey != null);
        return items;
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
