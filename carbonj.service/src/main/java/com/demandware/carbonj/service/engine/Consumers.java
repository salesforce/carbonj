/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.FileUtils;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Consumers {

    private static final Logger log = LoggerFactory.getLogger(Consumers.class);

    private final MetricRegistry metricRegistry;

    private final PointProcessor pointProcessor;

    private final KinesisConfig kinesisConfig;

    private final ConsumerRules consumerRules;

    private final Map<String, KinesisConsumer> consumers;

    private final NamespaceCounter namespaceCounter;

    private final File indexNameSyncDir;

    private final String activeProfile;

    private final KinesisAsyncClient kinesisAsyncClient;

    private final DynamoDbAsyncClient dynamoDbAsyncClient;

    private final CloudWatchAsyncClient cloudWatchAsyncClient;

    private final int kinesisConsumerRetroSeconds;

    Consumers(MetricRegistry metricRegistry, PointProcessor pointProcessor, File rulesFile, KinesisConfig kinesisConfig,
              NamespaceCounter namespaceCounter, File indexNameSyncDir, String activeProfile,
              KinesisAsyncClient kinesisAsyncClient, DynamoDbAsyncClient dynamoDbAsyncClient,
              CloudWatchAsyncClient cloudWatchAsyncClient, int kinesisConsumerRetroSeconds) {

        this.metricRegistry = metricRegistry;
        this.pointProcessor = pointProcessor;
        this.kinesisConfig = kinesisConfig;
        this.namespaceCounter = namespaceCounter;
        this.indexNameSyncDir = indexNameSyncDir;
        this.activeProfile = activeProfile;
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.cloudWatchAsyncClient = cloudWatchAsyncClient;
        this.kinesisConsumerRetroSeconds = kinesisConsumerRetroSeconds;
        consumers = new ConcurrentHashMap<>();
        consumerRules = new ConsumerRules(rulesFile);
        reload();
    }

    // synchronized to make sure that only one reload at a time.
    public synchronized void reload() {
        if (log.isDebugEnabled()) {
            log.debug(" Check for consumer configuration update");

        }

        try {
            Set<String> currentRules = consumerRules.getCurrentRules();
            Set<String> newRules = consumerRules.load();
            if (log.isDebugEnabled()) {
                log.debug(" Check for consumer configuration update");
            }
            if (!currentRules.equals(newRules)) {
                log.info(String.format("Updating consumer Rules. Old consumer rules: [%s], New consumer rules: [%s]", currentRules, newRules));
            }
            else {
                log.debug(" Consumer rules haven't changed.");
                return;
            }
            reconfigureConsumers(newRules, currentRules);
        } catch (Exception e) {
            log.error(" Failed to reload consumer config. Suppress. %s " , e);
        }
    }

    private void reconfigureConsumers(Set<String> newRules, Set<String> currentRules) {
        Set<String> obsoleteConsumers = new HashSet<>();
        Set<String> newConsumers = new HashSet<>();
        for (String consumer : currentRules) {
            log.info (consumer);

            if (newRules.contains(consumer)) {
                log.info(String.format("[%s] Reuse unchanged consumer", consumer));
                newConsumers.add(consumer);
                newRules.remove(consumer);
            } else {
                log.info(String.format("[%s] Consumer scheduled for removal", consumer));
                obsoleteConsumers.add(consumer);
            }

        }

        /* create new consumers */
        // we use the host name to generate the kinesis application name as they are stable for stable set pods.
        String hostName = getHostName();
        Date kinesisConsumerRetroDate = new Date(System.currentTimeMillis() - kinesisConsumerRetroSeconds * 1000L);
        for (String consumerName : newRules) {
            log.info(String.format("Creating new consumer with kinesis stream name: %s", consumerName));

            if (consumerName.startsWith("kinesis:")) {

                String kinesisStreamName = consumerName.substring(("kinesis:".length()));
                String kinesisApplicationName = getKinesisApplicationName(kinesisStreamName, hostName);
                String consumerCfgFile = "config/kinesis-" + kinesisStreamName + "-consumer.conf";

                try {
                    InputStream input = new FileInputStream(consumerCfgFile);
                    log.info(" Loading values from " + consumerCfgFile);
                    Properties consumerCfg = new Properties();
                    consumerCfg.load(input);
                    String kinesisApplicationNamePropValue = consumerCfg.getProperty("kinesis.application.name");
                    if( kinesisApplicationNamePropValue != null ) {
                        kinesisApplicationName = kinesisApplicationNamePropValue;
                    }
                } catch (FileNotFoundException e) {
                    log.warn(" config/" + consumerCfgFile + "not found in the classpath ");
                    log.info(" Falling back to default values ");
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }

                Counter initRetryCounter = metricRegistry.counter(MetricRegistry.name("kinesis.consumer." + kinesisStreamName + ".initRetryCounter"));
                StreamTracker streamTracker = new SingleStreamTracker(kinesisStreamName,
                        InitialPositionInStreamExtended.newInitialPositionAtTimestamp(kinesisConsumerRetroDate));
                ConfigsBuilder configsBuilder = new ConfigsBuilder(streamTracker, kinesisApplicationName, kinesisAsyncClient,
                        dynamoDbAsyncClient, cloudWatchAsyncClient, UUID.randomUUID().toString(),
                        new KinesisRecordProcessorFactory(metricRegistry, pointProcessor, kinesisConfig, kinesisStreamName));
                KinesisConsumer kinesisConsumer = new KinesisConsumer(kinesisStreamName, kinesisApplicationName, initRetryCounter, configsBuilder);
                log.info(String.format("New Consumer created with name %s", kinesisStreamName));
                newConsumers.add(consumerName);
                consumers.put(consumerName, kinesisConsumer);
            }
        }

        if (newConsumers.isEmpty()) {
            log.warn( "No kinesis consumers configured." );
        }
        consumerRules.putCurrentRules(newConsumers);
        close(obsoleteConsumers);
    }

    private String getHostName() {
        while (true) {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                log.error("Error while fetching host name", e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String getKinesisApplicationName(String streamName, String hostName)  {
        return streamName + "-" + hostName + "-" + activeProfile;
    }

    private void close(Set<String> consumerSet) {
        if (null == consumerSet) {
            return;
        }
        for (String consumer : consumerSet) {
            consumers.get(consumer).closeQuietly();
            consumers.remove(consumer);
        }
    }

    public void dumpStats() {
        Set<String> currentRules = consumerRules.getCurrentRules();
        for (String consumer : currentRules) {
            consumers.get(consumer).dumpStats();
        }
    }

    public void syncNamespaces() {
        File file = new File(indexNameSyncDir, "namespaces");
        try {
            FileUtils.dumpSetToFile(namespaceCounter.getLiveNamespaces(), file);
        } catch (IOException e) {
            log.error("Failed to dump namespace into file {} - {}", file.getAbsolutePath(), e.getMessage(), e);
        }
    }
}
