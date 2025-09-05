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
import java.util.concurrent.ConcurrentHashMap;

public class Consumers {

    private static final Logger log = LoggerFactory.getLogger(Consumers.class);

    private final MetricRegistry metricRegistry;

    private final PointProcessor pointProcessor;

    private final KinesisConfig kinesisConfig;
    private final CheckPointMgr<Date> checkPointMgr;

    private final ConsumerRules consumerRules;

    private final Map<String, KinesisConsumer> consumers;

    private final String kinesisConsumerRegion;

    private final PointProcessor recoveryPointProcessor;

    private final NamespaceCounter namespaceCounter;

    private final File indexNameSyncDir;

    private final String carbonjEnv;

    private final int kinesisConsumerTracebackSeconds;

    Consumers(MetricRegistry metricRegistry, PointProcessor pointProcessor, PointProcessor recoveryPointProcessor, File rulesFile,
              KinesisConfig kinesisConfig, CheckPointMgr<Date> checkPointMgr, String kinesisConsumerRegion,
              NamespaceCounter namespaceCounter, File indexNameSyncDir, String carbonjEnv, int kinesisConsumerTracebackSeconds) {

        this.metricRegistry = metricRegistry;
        this.pointProcessor = pointProcessor;
        this.recoveryPointProcessor = recoveryPointProcessor;
        this.kinesisConfig = kinesisConfig;
        this.checkPointMgr = checkPointMgr;
        this.kinesisConsumerRegion = kinesisConsumerRegion;
        this.namespaceCounter = namespaceCounter;
        this.indexNameSyncDir = indexNameSyncDir;
        this.consumers = new ConcurrentHashMap<>();
        this.consumerRules = new ConsumerRules(rulesFile);
        this.carbonjEnv = carbonjEnv;
        this.kinesisConsumerTracebackSeconds = kinesisConsumerTracebackSeconds;
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
                log.info("Updating consumer Rules. Old consumer rules: [{}], New consumer rules: [{}]", currentRules, newRules);
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
                log.info("[{}] Reuse unchanged consumer", consumer);
                newConsumers.add(consumer);
                newRules.remove(consumer);
            } else {
                log.info("[{}] Consumer scheduled for removal", consumer);
                obsoleteConsumers.add(consumer);
            }

        }

        /* create new consumers */
        // we use the host name to generate the kinesis application name as they are stable for stable set pods.
        String hostName = getHostName();
        for (String consumerName : newRules) {
            log.info("Creating new consumer with kinesis stream name: {}", consumerName);

            if (consumerName.startsWith("kinesis:")) {

                String kinesisStreamName = consumerName.substring(("kinesis:".length()));
                String kinesisApplicationName = getKinesisApplicationName(kinesisStreamName, hostName, carbonjEnv);
                log.info("Kinesis application name: {}", kinesisApplicationName);
                String consumerCfgFile = "config/kinesis-" + kinesisStreamName + "-consumer.conf";

                try {
                    InputStream input = new FileInputStream(consumerCfgFile);
                    log.info(" Loading values from {}", consumerCfgFile);
                    Properties consumerCfg = new Properties();
                    consumerCfg.load(input);
                    String kinesisApplicationNamePropValue = consumerCfg.getProperty("kinesis.application.name");
                    if( kinesisApplicationNamePropValue != null ) {
                        kinesisApplicationName = kinesisApplicationNamePropValue;
                    }
                } catch (FileNotFoundException e) {
                    log.warn("{} not found in the classpath ", consumerCfgFile);
                    log.info(" Falling back to default values ");
                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                }

                Counter initRetryCounter = metricRegistry.counter(MetricRegistry.name("kinesis.consumer." + kinesisStreamName + ".initRetryCounter"));
                KinesisConsumer kinesisConsumer = new KinesisConsumer(metricRegistry, pointProcessor, recoveryPointProcessor, kinesisStreamName,
                        kinesisApplicationName, kinesisConfig, checkPointMgr, initRetryCounter, kinesisConsumerRegion, kinesisConsumerTracebackSeconds);
                log.info("New Consumer created with name {}", kinesisStreamName);
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

    private String getKinesisApplicationName(String streamName, String hostName, String carbonjEnv)  {
        return streamName + "-" + hostName + "-" + carbonjEnv;
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
