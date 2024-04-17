/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.index.cfgMetricIndex;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.MetricProvider;
import com.demandware.carbonj.service.db.model.QueryCachePolicy;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.FileUtils;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.demandware.carbonj.service.ns.cfgNamespaces;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.util.function.Predicate;

@Configuration
@Import({cfgMetricIndex.class, cfgNamespaces.class})
public class cfgDataPoints
{
    @Value( "${metrics.store.longId:false}" )
    private boolean longId = false;

    @Value( "${metrics.store.dataDir:data}")
    private String dataDir = "data";

    @Value( "${metrics.store.stagingDir:work/carbonj-staging}")
    private String stagingDir = "work/carbonj-staging";

    @Value( "${metrics.store.lowerResolutionArchives.enabled:true}" )
    private boolean updateLowerResolutionArchives = true;

    @Value( "${metrics.store.stagingQueueSize:1000000}" )
    private int stagingQueueSize = 1000000;

    @Value("${metrics.store.stagingIntervalQueueConsumerBatchSize:10000}")
    int stagingIntervalQueueConsumerBatchSize = 10000;
    @Value("${metrics.store.stagingIntervalsQueueSizePerDb:1000000}")
    int stagingIntervalsQueueSizePerDb = 1000000;
    @Value("${metrics.store.stagingIntervalsQueueConsumersPerDb:1}")
    int stagingIntervalsQueueConsumersPerDb = 1;
    @Value("${metrics.store.intervalprocessor.emptyQueuePauseInMillis:100}")
    int emptyQueuePauseInMillis;

    @Value("${metrics.store.query.timeSeriesCacheMaxSize:100000}")
    int timeSeriesCacheMaxSize = 100000;
    @Value("${metrics.store.query.timeSeriesCacheExpireInSec:30}")
    int timeSeriesCacheExpireInSec = 30;

    // we can generalize this later. For now keeping it simple.
    @Value("${metrics.store.query.useTimeSeriesCacheFor60s24h:true}")
    boolean useTimeSeriesCacheFor60s24h = true;

    @Value("${metrics.store.query.useTimeSeriesCacheFor60s30d:true}")
    boolean useTimeSeriesCacheFor60s30d = true;

    @Value("${metrics.store.query.useTimeSeriesCacheFor5m7d:false}")
    boolean useTimeSeriesCacheFor5m7d = false;

    @Value("${metrics.store.query.useTimeSeriesCacheFor30m2y:false}")
    boolean useTimeSeriesCacheFor30m2y = false;


    @Value("${staging.systemSort.tmpDir:work}")
    String systemSortTmpDir = "work";

    @Value("${staging.systemSort.bufSizeKB:256000}")
    long systemSortBufSizeInKB = 256000;

    @Value("${staging.systemSort.nParallel:2}")
    int systemSortParallel = 2;

    @Value("${staging.systemSort.timeoutInSec:300}")
    long systemSortTimeoutInSeconds = 300;

    @Value("${metrics.store.query.disableNameSpaceCounterCheck:false}")
    boolean disableNameSpaceCounterCheck;

    @Autowired
    MetricRegistry metricRegistry;
    
    @Value("${staging.timeAggrJob.intervalInMins:30}")
    private int timeAggrJobIntervalInMins;

    @Value("${staging.timeAggrJob.threads:3}")
    private int timeAggrJobThreads;

    @Bean
    RocksDBConfig rocksDBConfig()
    {
        return new RocksDBConfig();
    }

    @Bean
    DataPointArchiveFactory pointArchiveFactory(RocksDBConfig dbConfig)
    {
        return new DataPointArchiveFactory(metricRegistry, dataDir(), dbConfig, longId);
    }

    private StagingFilesSort fileSort()
    {
        SystemSort sort = new SystemSort( systemSortTimeoutInSeconds );
        sort.setBufSizeKb( systemSortBufSizeInKB );
        sort.setTmpDir( systemSortTmpDir );
        sort.setParallel( systemSortParallel );
        return sort;
    }

    @Bean
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    StagingFiles stagingFiles(MetricProvider mProvider)
    {
        return new StagingFiles(metricRegistry, stagingDir(), fileSort(), mProvider);
    }

    @Bean
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    DataPointStagingStore pointStagingStore( StagingFiles stagingFiles)
    {
        return new DataPointStagingStore(metricRegistry, stagingFiles, stagingQueueSize,
            stagingIntervalQueueConsumerBatchSize, stagingIntervalsQueueSizePerDb, stagingIntervalsQueueConsumersPerDb,
                emptyQueuePauseInMillis, timeAggrJobIntervalInMins, timeAggrJobThreads);
    }

    @Bean
    DataPointStore dataPointStore(DataPointArchiveFactory dbFactory, DatabaseMetrics dbMetrics,
                                  @Autowired(required = false) DataPointStagingStore stagingStore, NamespaceCounter nsCounter)
    {
        QueryCachePolicy qcp = new QueryCachePolicy( useTimeSeriesCacheFor60s24h, useTimeSeriesCacheFor60s30d,
                useTimeSeriesCacheFor5m7d, useTimeSeriesCacheFor30m2y );

        Predicate<String> nameSpaceCounterCheck;
        if (disableNameSpaceCounterCheck) {
            nameSpaceCounterCheck = name -> true;
        } else {
            nameSpaceCounterCheck = nsCounter::exists;
        }

        return new DataPointStoreImpl(metricRegistry, dbFactory, dbMetrics, stagingStore, updateLowerResolutionArchives,
            timeSeriesCacheMaxSize, timeSeriesCacheExpireInSec, qcp, nameSpaceCounterCheck);
    }

    private File dataDir()
    {
        Preconditions.checkArgument( StringUtils.isNotEmpty( dataDir ),
                        "Dir path for databases with series data is not defined. Use '%s' property.",
                        "metrics.store.dataDir");
        return FileUtils.writableDir(dataDir);
    }

    private File stagingDir()
    {
        Preconditions.checkArgument( StringUtils.isNotEmpty( stagingDir ),
                        "Dir path for staging files is not defined. Use '%s' property.",
                        "metrics.store.stagingDir");
        return FileUtils.writableDir( stagingDir );
    }
}
