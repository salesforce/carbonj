/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.index.cfgMetricIndex;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.points.cfgDataPoints;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.cfgCentralThreadPools;
import com.demandware.carbonj.service.events.EventsLogger;
import com.demandware.carbonj.service.events.cfgCarbonjEventsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Import( { cfgMetricIndex.class, cfgDataPoints.class, cfgCentralThreadPools.class, cfgCarbonjEventsLogger.class } )
@ConditionalOnProperty(name=cfgTimeSeriesStorage.DB_ENABLED_PROPERTY_KEY, havingValue="true", matchIfMissing=true)
public class cfgTimeSeriesStorage
{
    private static Logger log = LoggerFactory.getLogger( cfgTimeSeriesStorage.class );

    public static final String DB_ENABLED_PROPERTY_KEY = "metrics.store.enabled";

    @Value( "${metrics.store.longId:false}" )
    private boolean longId;

    @Value( "${metrics.store.fetchSeriesThreads:20}" )
    private int nTaskThreads;

    @Value( "${metrics.store.threadBlockingQueueSize:100}" )
    private int threadBlockingQueueSize;

    @Value( "${metrics.store.heavyQueryThreads:5}" )
    private int nHeavyQueryThreads;

    @Value( "${metrics.store.heavyQueryBlockingQueueSize:10}" )
    private int heavyQueryBlockingQueueSize;

    @Value( "${metrics.store.fetchSeriesInBatches:true}" )
    private boolean batchedSeriesRetrieval;

    @Value( "${metrics.store.batchedSeriesSize:100}" )
    private int batchedSeriesSize;

    @Value( "${debug.dumpIndex:false}" )
    private boolean dumpIndex;

    @Value( "${debug.dumpIndexFile:index-data.out}" )
    private String dumpIndexFile;

    @Value( "${metrics.tasks.queueSize:500000}" )
    private int serialQueueSize = 500000;

    @Value( "${log.nonLeafPoints.quota.min:10}" )
    private int maxNonLeafPointsLoggedPerMin;

    @Value( "${metrics.store:config/service.properties}" )
    private String metricStoreConfigFile = "config/service.properties";

    @Autowired
    MetricRegistry metricRegistry;

    @Bean
    @DependsOn( "stringsCache" )
    TimeSeriesStore timeSeriesStore( MetricIndex nameIndex, DataPointStore pointStore, DatabaseMetrics dbMetrics,
                                     ScheduledExecutorService s, @Qualifier("CarbonjEventsLogger") EventsLogger logger) {
        log.info( String.format( "Creating TimeSeriesStore: nThreads = %s", nTaskThreads ) );
        TimeSeriesStoreImpl timeSeriesStore = new TimeSeriesStoreImpl( metricRegistry, nameIndex, logger,
                TimeSeriesStoreImpl.newMainTaskQueue( nTaskThreads, threadBlockingQueueSize ),
                TimeSeriesStoreImpl.newHeavyQueryTaskQueue( nHeavyQueryThreads, heavyQueryBlockingQueueSize ),
                TimeSeriesStoreImpl.newSerialTaskQueue( serialQueueSize ), pointStore,
            dbMetrics, batchedSeriesRetrieval,
            batchedSeriesSize, dumpIndex, new File( dumpIndexFile ), maxNonLeafPointsLoggedPerMin, metricStoreConfigFile,
                longId);

        s.scheduleWithFixedDelay(timeSeriesStore::reload, 60, 60, TimeUnit.SECONDS );
        s.scheduleWithFixedDelay(timeSeriesStore::refreshStats, 60, 10, TimeUnit.SECONDS );

        return timeSeriesStore;
    }
}
