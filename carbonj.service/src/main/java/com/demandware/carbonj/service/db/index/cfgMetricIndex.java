/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.StorageAggregationPolicySource;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.FileUtils;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.demandware.core.config.cfgMetric;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.demandware.carbonj.service.config.ConfigUtils.locateConfigFile;

@Configuration
@Import( cfgMetric.class )
public class cfgMetricIndex
{
    @Value( "${metrics.store.longId:false}" )
    private boolean longId;

    @Value( "${metrics.store.indexDir:}" )
    private String indexDir = null;

    @Value( "${metrics.store.dataDir:data}" )
    private String dataDir = null;

    @Value( "${metrics.store.nameIndexCacheSize:10000000}" )
    private int nameIndexMaxCacheSize = 10000000;

    @Value( "${metrics.cacheExpireAfterAccessInMinutes:120}" )
    private int metricCacheExpireAfterAccessInMinutes = 120;

    @Value( "${storage.aggregation.rules:config/storage-aggregation.conf}" )
    private String storageAggregationRulesConfigFile = "config/storage-aggregation.conf";

    @Value( "${metrics.store.queryCacheMaxSize:10000}" )
    private int nameIndexQueryCacheMaxSize;

    @Value( "${metrics.store.expireAfterWriteQueryCacheInSeconds:120}" )
    private int expireAfterWriteQueryCacheInSeconds;

    @Value( "${metrics.store.queryPatternCacheMaxSize:10000}" )
    private int nameIndexQueryPatternCacheMaxSize;

    @Value( "${metrics.store.expireAfterWriteQueryPatternCacheInSeconds:120}" )
    private int expireAfterWriteQueryPatternCacheInSeconds;

    @Value( "${metrics.store.enableIdCache:false}" )
    private boolean enableIdCache;

    @Value( "${metrics.store:config/application.properties}" )
    private String metricStoreConfigFile = "config/application.properties";

    @Value("${rocksdb.readonly:false}")
    private boolean rocksdbReadonly;

    @Value("${metrics.store.sync.secondary.db:false}")
    private boolean syncSecondaryDb;

    @Value("${rocksdb.catchup.retry:3}")
    private int catchupRetry = 3;

    @Value("${metrics.store.sync.queue.size.limit:10000}")
    private int nameIndexKeyQueueSizeLimit = 10000;

    @Value( "${log.invalidLeafMetrics.quota.min:10}" )
    private int maxInvalidLeafMetricsLoggedPerMin;

    // TODO duplicated in different cfg beans
    @Value( "${app.servicedir:}" )
    private String serviceDir;

    @Autowired
    MetricRegistry metricRegistry;

    @Bean
    @ConditionalOnProperty(name = "metrics.store.sync.secondary.db", havingValue = "true")
    File indexNameDbDir() {
        return dbDir("index-name");
    }

    @Bean( name = "metricNameIndexStore" )
    IndexStore<String, NameRecord> metricNameIndexStore()
    {
        File dbDir = dbDir( "index-name" );
        return new IndexStoreRocksDB<>( metricRegistry, "index-name", dbDir, new NameRecordSerializer(longId), rocksdbReadonly, catchupRetry);
    }

    @Bean( name = "metricIdIndexStore" )
    IndexStore<Long, IdRecord> metricIdIndexStore()
    {
        File dbDir = dbDir( "index-id" );
        return new IndexStoreRocksDB<>( metricRegistry,"index-id", dbDir, new IdRecordSerializer(longId), rocksdbReadonly, catchupRetry);
    }

    @Bean
    public StorageAggregationPolicySource storageAggregationPolicySource(ScheduledExecutorService s)
    {

        File rulesFile = locateConfigFile( serviceDir, storageAggregationRulesConfigFile );
        StorageAggregationRulesLoader rulesLoader = new StorageAggregationRulesLoader( rulesFile );
        s.scheduleWithFixedDelay(rulesLoader::reload, 60, 45, TimeUnit.SECONDS );

        StorageAggregationPolicySource policySource = new StorageAggregationPolicySource( rulesLoader );
        s.scheduleWithFixedDelay(policySource::cleanup, 10, 120, TimeUnit.MINUTES );
        return policySource;
    }


    @Bean
    MetricIndex metricIndex(@Qualifier( "metricNameIndexStore" ) IndexStore<String, NameRecord> nameIndex,
                            @Qualifier( "metricIdIndexStore" ) IndexStore<Long, IdRecord> idIndex,
                            DatabaseMetrics dbMetrics, NameUtils nameUtils,
                            StorageAggregationPolicySource policySource, ScheduledExecutorService s,
                            NamespaceCounter namespaceCounter)
    {
        MetricIndexImpl metricIndex = new MetricIndexImpl(metricRegistry, metricStoreConfigFile, nameIndex, idIndex, dbMetrics,
                nameIndexMaxCacheSize, metricCacheExpireAfterAccessInMinutes, nameUtils, policySource,
                nameIndexQueryCacheMaxSize, expireAfterWriteQueryCacheInSeconds, enableIdCache, longId,
                namespaceCounter, rocksdbReadonly, syncSecondaryDb, nameIndexKeyQueueSizeLimit,
                nameIndexQueryPatternCacheMaxSize, expireAfterWriteQueryPatternCacheInSeconds, maxInvalidLeafMetricsLoggedPerMin);
        s.scheduleWithFixedDelay(metricIndex::reload, 300, 300, TimeUnit.SECONDS);
        return metricIndex;
    }

    private File dbDir( String dbName )
    {
        String parentDir = StringUtils.isNotEmpty( indexDir ) ? indexDir : dataDir;
        Preconditions.checkArgument( StringUtils.isNotEmpty( parentDir ),
            "Dir path for database [%s] is not defined. Use '%s' or '%s' properties.", dbName, "metrics.store.dataDir",
            "metrics.store.indexDir" );
        File dir = FileUtils.writableDir( parentDir );
        return new File( dir, dbName );
    }
}
