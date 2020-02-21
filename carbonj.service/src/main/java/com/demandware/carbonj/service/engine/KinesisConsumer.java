/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.kcl.MemLeaseManager;
import com.demandware.carbonj.service.engine.recovery.*;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    private Worker worker;

    private PointProcessor recoveryPointProcessor;

    private volatile boolean closed;

    public KinesisConsumer(MetricRegistry metricRegistry, PointProcessor pointProcessor, PointProcessor recoveryPointProcessor,
                           String kinesisStreamName, String kinesisApplicationName,
                           KinesisConfig kinesisConfig, CheckPointMgr<Date> checkPointMgr,
                           Counter noOfRestarts) {
        this.metricRegistry = metricRegistry;
        this.pointProcessor = Preconditions.checkNotNull(pointProcessor);
        this.recoveryPointProcessor = recoveryPointProcessor;
        this.kinesisStreamName = kinesisStreamName;
        this.kinesisApplicationName = kinesisApplicationName;
        this.kinesisConfig = kinesisConfig;
        this.checkPointMgr =  Preconditions.checkNotNull(checkPointMgr);
        this.noOfRestarts = noOfRestarts;
        log.info("Kinesis consumer started");
        this.start();
    }

    public void run () {

        while (!closed) {
            try {
                AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
                String workerId = kinesisApplicationName + "-worker";

                if (kinesisConfig.isRecoveryEnabled()) {
                    initCatchupKinesisClient();
                }

                KinesisClientLibConfiguration kinesisClientLibConfiguration =
                        new KinesisClientLibConfiguration(kinesisApplicationName, kinesisStreamName, credentialsProvider,
                                workerId)
                                .withInitialPositionInStream(InitialPositionInStream.LATEST)
                                .withFailoverTimeMillis(kinesisConfig.getLeaseExpirationTimeInSecs() * 1000);

                int maxRecords = kinesisConfig.getMaxRecords();
                if (maxRecords > 0) {
                    kinesisClientLibConfiguration.withMaxRecords(maxRecords);
                }

                log.info(" Kinesis Client Library started with application name " + kinesisApplicationName + " with stream "
                        + kinesisStreamName + " and worker id is " + workerId);

                IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(metricRegistry, pointProcessor,
                        kinesisConfig, kinesisStreamName);
                worker = new Worker.Builder()
                        .recordProcessorFactory(recordProcessorFactory)
                        .config(kinesisClientLibConfiguration)
                        .leaseManager(new MemLeaseManager(kinesisConfig.getLeaseTakerDelayInMillis()))
                        .build();
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

    private void shutdownQuietly(Worker worker) {
        try {
            if (worker != null) {
                worker.shutdown();
            }
        } catch (Throwable throwable) {
            log.error("worker shutdown failed!", throwable);
        }
    }

    private void initCatchupKinesisClient() throws Exception {
        log.info("Initializing kinesis recovery processing..");
        GapsTable gapsTable;
        if( kinesisConfig.getCheckPointProvider() == KinesisRecoveryProvider.DYNAMODB ) {
            gapsTable = new DynamoDbGapsTableImpl( AmazonDynamoDBClientBuilder.standard().build(), kinesisApplicationName, kinesisConfig.getGapsTableProvisionedThroughput() );
        } else {
            gapsTable = new FileSystemGapsTableImpl(kinesisConfig.getCheckPointDir());
        }

        AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.defaultClient();
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
        log.info(String.format("Kinesis stream %s consumer stopped", kinesisStreamName));
    }

    public void dumpStats() {
        log.info( String.format( "Metrics consumed in kinesis stream %s =%s", kinesisStreamName,
                KinesisRecordProcessorFactory.metricsReceived.getCount() ));
        log.info( String.format( "Messages consumed in kinesis stream %s = %s",  kinesisStreamName,
                KinesisRecordProcessorFactory.messagesReceived.getCount() ));
    }
}
