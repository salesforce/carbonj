/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.log.CompletedQueryStats;
import com.demandware.carbonj.service.db.log.QueryStats;
import com.demandware.carbonj.service.db.log.Stats;
import com.demandware.carbonj.service.db.model.DataPointExportResults;
import com.demandware.carbonj.service.db.model.DataPointImportResults;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.db.model.DeleteAPIResult;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.model.TooManyDatapointsFoundException;
import com.demandware.carbonj.service.db.model.TooManyMetricsFoundException;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.Quota;
import com.demandware.carbonj.service.engine.BlockingPolicy;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.engine.DrainUtils;
import com.demandware.carbonj.service.engine.Query;
import com.demandware.carbonj.service.engine.ResponseStream;
import com.demandware.carbonj.service.events.CarbonjEvent;
import com.demandware.carbonj.service.events.Constants;
import com.demandware.carbonj.service.events.EventsLogger;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.demandware.carbonj.service.db.util.DatabaseMetrics.nonLeafMetricsReceived;

public class TimeSeriesStoreImpl implements TimeSeriesStore
{
    private static final Logger log = LoggerFactory.getLogger(TimeSeriesStoreImpl.class);

    private static final String DEFAULT_HEAVY_QUERY_THRESHOLD = String.valueOf(10000 * 24 * 60);

    private static final String DEFAULT_MAX_DATA_POINTS_PER_REQUEST = String.valueOf(Integer.MAX_VALUE);

    private static Counter rejectedCounter;

    private static Histogram activeFetchThreadsHistogram;

    private static final String DEFAULT_LOGGER_TIME_THRESHOLD_MILLIS = String.valueOf(5000);  // % seconds

    private static final String DEFAULT_LOGGER_SERIES_THRESHOLD = String.valueOf(5000);

    @SuppressWarnings("unused") private static Gauge<Number> activeMeanFetchThreadCount;

    private static Meter lightQueries;

    private static Meter heavyQueries;

    private final Quota nonLeafPointsLogQuota;

    private final MetricIndex nameIndex;

    private final EventsLogger<CarbonjEvent> eventLogger;

    private final DataPointStore pointStore;

    private final DatabaseMetrics dbMetrics;

    private final int batchedSeriesSize;

    private final ThreadPoolExecutor ex;

    private final ThreadPoolExecutor heavyQueryTaskQueue;

    /**
     * executor service for the tasks that require concurrency handling use queuing mechanism as an alternative to lock
     * based synchronization
     */
    private final ThreadPoolExecutor serialTaskQueue;

    private final boolean dumpIndex;

    private final File dumpIndexFile;

    private final String metricsStoreConfigFile;

    /**
     * Maximum number of data points that can be returned as part of query.
     */
    private volatile long dataPointsThreshold;

    /**
     * the number of data points beyond which a query that will return these many number of points will be considered
     * as a heavy query.
     */
    private volatile long heavyQueryThreshold;

    private volatile long logResponseTimeThresholdInMillis;

    private volatile long logNoOfSeriesThreshold;

    private final boolean longId;

    private final boolean rocksdbReadonly;

    public static ThreadPoolExecutor newSerialTaskQueue(int queueSize) {
        ThreadFactory tf =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setPriority(Thread.MAX_PRIORITY - 1)
                        .setNameFormat("TimeSeriesStore.SerialTaskPool %d")
                        .setUncaughtExceptionHandler(
                                (th, e) -> log.error("TimeSeriesStore.SerialTaskPool UncaughtException", e)).build();
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(queueSize),
                tf, new ThreadPoolExecutor.AbortPolicy() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                rejectedCounter.inc();
                super.rejectedExecution(r, e);
            }
        });
    }

    public static ThreadPoolExecutor newMainTaskQueue(int nThreads, int threadBlockingQueueSize) {
        Preconditions.checkArgument(nThreads > 0);
        return new ThreadPoolExecutor(nThreads, nThreads, 1, TimeUnit.HOURS, new LinkedBlockingQueue<>(
                threadBlockingQueueSize),
                new BlockingPolicy("SeriesStreamingQueue", null, DatabaseMetrics.seriesStreamingQueueBlockingTimer, true));
    }

    public static ThreadPoolExecutor newHeavyQueryTaskQueue(int nThreads, int threadBlockingQueueSize) {
        Preconditions.checkArgument(nThreads > 0);
        return new ThreadPoolExecutor(nThreads, nThreads, 1, TimeUnit.HOURS, new LinkedBlockingQueue<>(
                threadBlockingQueueSize),
                new BlockingPolicy("heavyQueryQueue", null, DatabaseMetrics.heavyQueryQueueBlockingTimer, true));
    }

    public TimeSeriesStoreImpl(MetricRegistry metricRegistry, MetricIndex nameIndex, EventsLogger<CarbonjEvent> eventLogger,
                               ThreadPoolExecutor mainTaskQueue,
                               ThreadPoolExecutor heavyQueryTaskQueue, ThreadPoolExecutor serialTaskQueue,
                               DataPointStore pointStore, DatabaseMetrics dbMetrics,
                               boolean batchedSeriesRetrieval, int batchedSeriesSize, boolean dumpIndex,
                               File dumpIndexFile, int maxNonLeafPointsLoggedPerMin, String metricsStoreConfigFile,
                               boolean longId) {
        this(metricRegistry, nameIndex, eventLogger, mainTaskQueue, heavyQueryTaskQueue, serialTaskQueue, pointStore,
                dbMetrics, batchedSeriesRetrieval, batchedSeriesSize, dumpIndex, dumpIndexFile, maxNonLeafPointsLoggedPerMin,
                metricsStoreConfigFile, longId, false);
    }

    public TimeSeriesStoreImpl(MetricRegistry metricRegistry, MetricIndex nameIndex, EventsLogger<CarbonjEvent> eventLogger,
                               ThreadPoolExecutor mainTaskQueue,
                               ThreadPoolExecutor heavyQueryTaskQueue, ThreadPoolExecutor serialTaskQueue,
                               DataPointStore pointStore, DatabaseMetrics dbMetrics,
                               boolean batchedSeriesRetrieval, int batchedSeriesSize, boolean dumpIndex,
                               File dumpIndexFile, int maxNonLeafPointsLoggedPerMin, String metricsStoreConfigFile,
                               boolean longId, boolean rocksdbReadonly) {
        this.nameIndex = Preconditions.checkNotNull(nameIndex);
        this.eventLogger = eventLogger;
        this.pointStore = Preconditions.checkNotNull(pointStore);
        this.dbMetrics = Preconditions.checkNotNull(dbMetrics);
        this.ex = Preconditions.checkNotNull(mainTaskQueue);
        this.heavyQueryTaskQueue = Preconditions.checkNotNull(heavyQueryTaskQueue);
        this.serialTaskQueue = Preconditions.checkNotNull(serialTaskQueue);
        this.batchedSeriesSize = batchedSeriesSize;
        this.dumpIndex = dumpIndex;
        this.dumpIndexFile = dumpIndexFile;
        this.nonLeafPointsLogQuota = new Quota(maxNonLeafPointsLoggedPerMin, 60);
        this.longId = longId;
        this.rocksdbReadonly = rocksdbReadonly;

        rejectedCounter = metricRegistry.counter(
                MetricRegistry.name("timeSeriesStore.serialTaskQueue", "rejects"));

        activeFetchThreadsHistogram = metricRegistry.histogram(
                MetricRegistry.name("fetchThreads", "activeThreadsHist" ));

        activeMeanFetchThreadCount = metricRegistry.register(
                MetricRegistry.name("fetchThreads", "activeThreadsMean"), ( ) -> activeFetchThreadsHistogram.getSnapshot().getMean() );

       lightQueries = metricRegistry.meter(
                MetricRegistry.name( "lightQueries", "received" ) );

       heavyQueries = metricRegistry.meter(
                MetricRegistry.name( "heavyQueries", "received" ) );




        String name = MetricRegistry.name("fetchThreads", "activeThreads" );
        registerActiveThreadsGauge(metricRegistry, name, ex);

        name = MetricRegistry.name("heavyQueryThreads", "activeThreads" );
        registerActiveThreadsGauge(metricRegistry, name, heavyQueryTaskQueue);

        this.metricsStoreConfigFile = metricsStoreConfigFile;

        loadFromConfigFile(metricsStoreConfigFile);
    }

    private void registerActiveThreadsGauge(MetricRegistry registry, String name,
                                            ThreadPoolExecutor threadPoolExecutor) {
        registry.remove(name);
        @SuppressWarnings("unused") Gauge<Number> gauge = registry.register(name, threadPoolExecutor::getActiveCount);
    }

    @Override
    public void dumpIndex(File file) {
        this.nameIndex.dumpIndex(file);
    }

    @PostConstruct
    private void init() {
        openDatabases();
        if (dumpIndex) {
            log.info("writing index content to file: [" + dumpIndexFile + "]");
            nameIndex.dumpIndex(dumpIndexFile);
        }
    }

    private void openDatabases() {
        nameIndex.open();
        if (!rocksdbReadonly) {
            pointStore.open();
        }
    }

    @Override
    public void put(DataPoints points) {
        assignMetrics(points, m -> m.getHighestPrecisionArchive().orElse(null), (i, dp) -> {
            // if it is a metric with a new name we
            // 1. Create new point instance (original point belongs to a batch that will be processed on another thread.
            // 2. asynchronously create a new name in the name index
            // 3. resubmit the data point copy for creation as it will not be stored the first time
            try {
                DataPoint dp2 = new DataPoint(dp.name, dp.val, dp.ts, false);
                dp.drop();
                if (dp2.name != null && dp2.name.startsWith("pod222.ecom_ag.bjmr.bjmr_prd") && dp2.name.endsWith("number-of-filters.max")) {
                    log.warn("============");
                    log.warn("Put aggregated metric " + dp2.name);
                    log.warn(dp2.toString());
                    log.warn("============");
                }
                serialTaskQueue.submit(() -> {
                    if (null != nameIndex.createLeafMetric(dp2.name)) {
                        this.accept(new DataPoints(List.of(dp2)));
                    }
                });
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("New metrics queue is full - failed to save " + dp);
                }
            }
        });
        // 2. insert data points
        pointStore.insertDataPoints(points);
    }

    @Override
    public void importPoints(String dbName, DataPoints points) {
        RetentionPolicy p = RetentionPolicy.getInstanceForDbName(dbName);
        assignMetrics(points, p, (i, dp) -> {
            // if it is a metric with a new name we
            // 1. create a new name in the name index
            // 2. wait for completion and move forward
            try {
                Metric m = serialTaskQueue.submit(() -> nameIndex.createLeafMetric(dp.name)).get();
                points.assignMetric(i, m, p);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("New metrics queue is full - failed to save " + dp);
                }
            }
        });
        // 2. insert data points
        pointStore.importDataPoints(dbName, points);
    }

    private void assignMetrics(DataPoints points, RetentionPolicy policy,
                               BiConsumer<Integer, DataPoint> newNamesHandler) {
        assignMetrics(points, m -> policy, newNamesHandler);
    }

    private void assignMetrics(DataPoints points, Function<Metric, RetentionPolicy> policyResolver,
                               BiConsumer<Integer, DataPoint> newNamesHandler) {
        DataPoint t;
        Metric m;
        for (int i = 0, n = points.size(); i < n; i++) {
            t = points.get(i);
            if (t.isValid()) {
                m = resolveMetric(t);
                if (m != null) {
                    if (m.isLeaf()) {
                        points.assignMetric(i, m, policyResolver);
                    } else {
                        if(nonLeafPointsLogQuota.allow()) {
                            log.info(String.format("dropping point received for non-leaf metric. Point [%s] ", t));
                        }
                        nonLeafMetricsReceived.mark();
                        t.drop();
                    }
                } else {
                    newNamesHandler.accept(i, t);
                }
            }
        }
    }

    private Metric resolveMetric(DataPoint t) {
        Metric metric = null;

        try {
            metric = nameIndex.getMetric(t.name);
        } catch (Exception e) {
            dbMetrics.markError();
        }

        return metric;
    }

    @Override
    public List<Metric> findMetrics(String pattern, boolean leafOnly, boolean useThreshold, boolean skipInvalid) {
        long startTime = System.currentTimeMillis();
        try(final Timer.Context ignored = DatabaseMetrics.findMetricsTimer.time())
        {
            List<Metric> metrics = nameIndex.findMetrics(pattern, leafOnly, useThreshold, skipInvalid);
            long currentTimeMillis = System.currentTimeMillis();
            long responseTime = currentTimeMillis - startTime;
            if (responseTime > logResponseTimeThresholdInMillis) {
                eventLogger.log(new FindMetricsStats(pattern, responseTime, metrics.size(), currentTimeMillis));
            }
            return metrics;
        }
        catch (TooManyMetricsFoundException e) {
            eventLogger.log(new FindSeriesLimitExceededEvent(pattern, e.getLimit(), System.currentTimeMillis()));
            throw e;
        }
    }

    @Override
    public List<Metric> findMetrics(String pattern) {
        return nameIndex.findMetrics(pattern, false, false, true);
    }

    @Override
    public Metric getMetric(String name) {
        return nameIndex.getMetric(name);
    }

    @Override
    public Metric getMetric(String name, boolean createIfMissing) {
        Metric m = nameIndex.getMetric(name);
        if (m == null && createIfMissing) {
            m = nameIndex.createLeafMetric(name);
        }
        return m;
    }

    @Override
    public DataPointImportResults importPoints(String dbName, List<DataPoint> points, int maxImportErrors) {
        if (!RetentionPolicy.dbNameExists(dbName)) {
            throw new RuntimeException(String.format("Unknown dbName [%s]", dbName));
        }

        return pointStore.importDataPoints(dbName, points, maxImportErrors);
    }

    @Override
    public DataPointExportResults exportPoints(String dbName, String metricName) {
        Metric m = nameIndex.getMetric(metricName);
        if (m == null) {
            throw new RuntimeException(String.format("Metric [%s] not found", metricName));
        }
        if (!m.isLeaf()) {
            throw new RuntimeException(String.format("Metric [%s] is not a leaf metric.", metricName));
        }

        return exportPoints(dbName, metricName, m.id);
    }

    @Override
    public DataPointExportResults exportPoints(String dbName, long metricId) {
        return exportPoints(dbName, null, metricId);
    }

    private DataPointExportResults exportPoints(String dbName, String metricName, Long metricId) {
        if (!RetentionPolicy.dbNameExists(dbName)) {
            throw new RuntimeException(String.format("Unknown dbName [%s]", dbName));
        }

        return new DataPointExportResults(dbName, metricName, pointStore.getValues(
                RetentionPolicy.getInstanceForDbName(dbName), metricId, 0, Integer.MAX_VALUE));
    }

    @Override
    public List<DataPointValue> getValues(String dbName, String metricName, int from, int to) {
        Metric m = nameIndex.getMetric(metricName);
        if (null == m) {
            return Collections.emptyList();
        }
        return pointStore.getValues(RetentionPolicy.getInstanceForDbName(dbName), m.id, from, to);
    }

    @Override
    public long deleteDataPoints( String dbName, int ts )
    {
        return pointStore.delete( dbName, ts );
    }

    @Override
    public List<Series> fetchSeriesData( Query query)
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "received query for series data. query: " + query);
        }

        List<Series> seriesList = new ArrayList<>();
        try (Timer.Context ignored = DatabaseMetrics.getSeriesTimer.time())
        {
            List<Metric> leafMetrics = nameIndex.findMetrics(query.pattern(), true, true, true);

            ThreadPoolExecutor threadPoolExecutor = selectThreadPoolExecutor(query, leafMetrics);

            if ( threadPoolExecutor == null )
            {
                leafMetrics.forEach( m -> seriesList.add(pointStore.getSeries(m, query.from(), query.until(), query.now())));
            }
            else
            {
                List<Future<List<Series>>> futures = new ArrayList<>();
                //TODO: test on empty list
                for(List<Metric> batch : Lists.partition(leafMetrics, batchedSeriesSize))
                {
                    futures.add(threadPoolExecutor.submit( new GetSeriesTask( pointStore, batch, query.from(), query.until(), query.now() )));
                }

                try
                {
                    for ( Future<List<Series>> f : futures )
                    {
                        seriesList.addAll( f.get() );
                    }
                }
                catch ( InterruptedException e )
                {
                    log.error("Error: ", e );
                    futures.forEach( ( f ) -> f.cancel( true ) );
                }
                catch ( ExecutionException e )
                {
                    log.error("Error: ", e );
                }
            }
        }
        return seriesList;
    }

    long getEstimatedNumberOfDataPoints(int from, int until, int now, List<Metric> leafMetrics) {
        Optional<RetentionPolicy> retentionPolicy = RetentionPolicy.pickArchiveForQuery(from, until, now);
        if (retentionPolicy.isPresent()) {
            long maxNoOfPoints = retentionPolicy.get().maxPoints(from, until, now);
            return maxNoOfPoints * leafMetrics.size();
        }
        return heavyQueryThreshold + 1;  // don't know the retention policy.  send it to heavy query queue.
    }

    @Override
    @SuppressWarnings("unused")
    public void streamSeriesData(Query query, ResponseStream seriesStream )
    {
        long startTime = System.currentTimeMillis();
        Preconditions.checkNotNull( ex );

        if ( log.isDebugEnabled() )
        {
            log.debug( "received query for series data. " + query);
        }

        QueryDurations d = new QueryDurations();
        List<Metric> matchedLeafMetrics = Collections.emptyList();
        try (Timer.Context t = DatabaseMetrics.getSeriesTimer.time())
        {
            seriesStream.openSeriesList();
            matchedLeafMetrics = nameIndex.findMetrics(query.pattern(), true, true, true);

            ThreadPoolExecutor threadPoolExecutor = selectThreadPoolExecutor(query, matchedLeafMetrics);

            List<Future<BatchStats>> futures = new ArrayList<>();
            List<List<Metric>> batches = Lists.partition(matchedLeafMetrics, batchedSeriesSize);
            DatabaseMetrics.getSeriesTasksPerQuery.mark(batches.size());
            for(List<Metric> batch : batches)
            {
                StreamSeriesBatchMetricsTask task = new StreamSeriesBatchMetricsTask(pointStore, batch,
                        query, seriesStream, d);
                futures.add( threadPoolExecutor.submit( task ) );
            }
            // Ensure all executors complete the stream
            QueryStats queryStats = waitFor( futures, query, matchedLeafMetrics.size() );
            seriesStream.closeSeriesList();

            long currentTimeMillis = System.currentTimeMillis();
            long responseTime = currentTimeMillis - startTime;
            int noOfSeries = matchedLeafMetrics.size();
            if (responseTime > logResponseTimeThresholdInMillis || noOfSeries > logNoOfSeriesThreshold) {
                eventLogger.log(new CompletedQueryStats(query, noOfSeries,
                        threadPoolExecutor == heavyQueryTaskQueue, currentTimeMillis, queryStats));
            }

            DatabaseMetrics.getSeriesReadTimer.update(queryStats.getSeriesReadTimeMillis().getSum(), TimeUnit.MILLISECONDS);
            DatabaseMetrics.getSeriesSendTimer.update(queryStats.getSeriesWriteTimeMillis().getSum(), TimeUnit.MILLISECONDS);
        }
        catch(Throwable t)
        {
            if (DatabaseMetrics.getSeriesErrors != null) {
                DatabaseMetrics.getSeriesErrors.mark();
            }

            long time = System.currentTimeMillis();
            eventLogger.log(new FailedQueryStats(query, matchedLeafMetrics.size(), time, t));

            throw new RuntimeException(t);
        }
        finally
        {
            try
            {
                seriesStream.close();
            }
            catch (IOException e)
            {
                DatabaseMetrics.getSeriesErrors.mark();
                log.error("Failed to close response stream.", e);
            }
        }
    }

    private ThreadPoolExecutor selectThreadPoolExecutor(Query query, List<Metric> matchedLeafMetrics) {
        long estimatedNumberOfDataPoints = getEstimatedNumberOfDataPoints(query.from(), query.until(),
                query.now(), matchedLeafMetrics);
        if (estimatedNumberOfDataPoints > dataPointsThreshold) {
            throw new TooManyDatapointsFoundException(dataPointsThreshold);
        }

        ThreadPoolExecutor threadPoolExecutor;
        if (estimatedNumberOfDataPoints > heavyQueryThreshold) {
            threadPoolExecutor = heavyQueryTaskQueue;
            heavyQueries.mark();
        } else {
            threadPoolExecutor = ex;
            lightQueries.mark();
        }
        return threadPoolExecutor;
    }

    private QueryStats waitFor(List<Future<BatchStats>> futures, Query query, int noOfSeries)
    {
        int totalNoOfDataPoints = 0;
        LongSummaryStatistics waitTimeStats = new LongSummaryStatistics();
        LongSummaryStatistics readTimeStats = new LongSummaryStatistics();
        LongSummaryStatistics sendTimeStats = new LongSummaryStatistics();
        LongSummaryStatistics emptySeriesReadTimeStats = new LongSummaryStatistics();

        try
        {
            for ( Future<BatchStats> f : futures )
            {
                BatchStats stats = f.get();
                totalNoOfDataPoints += stats.noOfDataPoints;
                waitTimeStats.accept(stats.waitTimeMillis);
                readTimeStats.combine(stats.seriesReadTimeMillis);
                sendTimeStats.combine(stats.seriesWriteTimeMillis);
                emptySeriesReadTimeStats.combine(stats.emptySeriesReadTimeMillis);
            }
            DatabaseMetrics.pointsRead.mark(totalNoOfDataPoints);
        }
        catch ( InterruptedException e )
        {
            DatabaseMetrics.getSeriesWaitForTasksErrors.mark();
            log.error("Error: ", e );
            futures.forEach( ( f ) -> f.cancel( true ) );
            eventLogger.log(new FailedQueryStats(query, noOfSeries, System.currentTimeMillis(), e));
        }
        catch ( ExecutionException e )
        {
            DatabaseMetrics.getSeriesWaitForTasksErrors.mark();
            log.error("Error: ", e );
            eventLogger.log(new FailedQueryStats(query, noOfSeries, System.currentTimeMillis(), e));
        }
        return new QueryStats(totalNoOfDataPoints, waitTimeStats, readTimeStats, sendTimeStats, emptySeriesReadTimeStats);
    }

    @Override
    public void dumpStats()
    {
        log.info( "points saved: " + DatabaseMetrics.pointsSaved.getCount() );
        log.info( "metrics saved: " + DatabaseMetrics.metricsSaved.getCount() );
        log.info( "queries served: " + DatabaseMetrics.queriesServed.getCount() );
        // database stats
        nameIndex.dumpStats();
        pointStore.dumpStats();
    }

    @Override
    public void accept( DataPoints points )
    {
        put( points );
    }

    @Override
    public Metric selectRandomMetric()
    {
        return nameIndex.selectRandomMetric();
    }

    @PreDestroy
    private void destroy()
    {
        log.info( "stopping executors" );
        try
        {
            ex.shutdown();
            ex.awaitTermination( 5, TimeUnit.SECONDS );
        }
        catch ( Throwable e )
        {
            log.error( "Failure stopping main taskQueue", e );
        }
        try
        {
            serialTaskQueue.shutdown();
            serialTaskQueue.awaitTermination( 5, TimeUnit.SECONDS );
        }
        catch ( Throwable e )
        {
            log.error( "Failure stopping serial task queue", e );
        }
        closeDatabase();
    }

    private void closeDatabase()
    {
        log.info( "closing databases" );
        nameIndex.close();
        if (!rocksdbReadonly) {
            pointStore.close();
        }
        log.info( "all databases closed" );
    }

    @Override
    public List<Metric> deleteMetric( String name, boolean force, boolean testRun )
    {
        List<Metric> metrics = nameIndex.deleteMetric( name, force, testRun );
        if ( !testRun )
        {
            pointStore.delete( metrics );
        }
        return metrics;
    }

    @Override
    public DeleteAPIResult deleteAPI( String name, boolean delete, Set<String> exclude ) {
        return nameIndex.deleteAPI(name, delete, exclude );
    }

    @Override
    public Metric getMetric( long metricId )
    {
        return nameIndex.getMetric( metricId );
    }

    @Override
    public String getMetricName( long metricId )
    {
        return nameIndex.getMetricName( metricId );
    }

    @Override
    public void deleteAll()
    {
        for ( String m : nameIndex.getTopLevelNames() )
        {
            deleteMetric( m, true, false );
        }
    }

    @Override
    public void scanMetrics( Consumer<Metric> m )
    {
        if(longId)
        {
            scanMetrics( 0, Long.MAX_VALUE, m );
        }
        else
        {
            scanMetrics( 0, Integer.MAX_VALUE, m );
        }
    }

    @Override
    public long scanMetrics( long start, long end, Consumer<Metric> m )
    {
        return nameIndex.scanNames( start, end, m );
    }

    @Override
    public void drain()
    {
        DrainUtils.drain( ex );
        DrainUtils.drain( serialTaskQueue );
    }

    @Override
    public DataPointValue getFirst( String dbName, String metricName, int from, int to )
    {
        Metric m = nameIndex.getMetric( metricName );
        if ( null == m )
        {
            return null;
        }
        return pointStore.getFirst( RetentionPolicy.getInstanceForDbName( dbName ), m.id, from, to );
    }

    @Override
    public void refreshStats() {
        activeFetchThreadsHistogram.update(ex.getActiveCount());
    }

    private void loadFromConfigFile(String metricsStoreConfigFileName) {
        File metricStoreConfigFile = new File(metricsStoreConfigFileName);
        Properties properties = new Properties();
        try {
            if (metricStoreConfigFile.exists()) {
                try (FileReader reader = new FileReader(metricStoreConfigFile)) {
                    properties.load(reader);
                }
            }
        } catch (Exception e) {
            log.warn("Error while loading metric store configuration", e);
        }

        dataPointsThreshold = Long.parseLong(properties.getProperty("metrics.store.maxDataPointsPerRequest", DEFAULT_MAX_DATA_POINTS_PER_REQUEST));
        heavyQueryThreshold = Long.parseLong(properties.getProperty("metrics.store.heavyQueryThreshold", DEFAULT_HEAVY_QUERY_THRESHOLD));
        logResponseTimeThresholdInMillis = Long.parseLong(properties.getProperty("logger.query.responseTimeThreshold", DEFAULT_LOGGER_TIME_THRESHOLD_MILLIS));
        logNoOfSeriesThreshold = Long.parseLong(properties.getProperty("logger.query.seriesThreshold", DEFAULT_LOGGER_SERIES_THRESHOLD));
    }

    public void reload() {
        loadFromConfigFile(metricsStoreConfigFile);
    }

    private static class FailedQueryStats extends Stats implements CarbonjEvent
    {
        final long from;
        final long to;
        final boolean completed;
        final String stackTrace;

        FailedQueryStats(Query query, int noOfSeries, long time, Throwable e)
        {
            super(Constants.ERRORS_LOG_TYPE, "render", query.pattern(),
                    time - query.receivedTimeInMillis(), noOfSeries, time);
            this.from = query.from();
            this.to = query.until();
            this.completed = false;
            if (e != null) {
                stackTrace = ExceptionUtils.getStackTrace(e);
            } else {
                stackTrace = null;
            }
        }
    }

    private static class FindMetricsStats extends Stats implements CarbonjEvent
    {

        FindMetricsStats(String query, long responseTime, int noOfMetrics, long time)
        {
            super(Constants.QUERY_LOG_TYPE, "findMetrics", query, responseTime, noOfMetrics, time);
        }
    }

    private static class FindSeriesLimitExceededEvent implements CarbonjEvent
    {
        private final String pattern;
        private final int limit;
        private final long time;
        private final String type = Constants.ERRORS_LOG_TYPE;

        public FindSeriesLimitExceededEvent(String pattern, int limit, long time)
        {
            this.pattern = pattern;
            this.limit = limit;
            this.time = time;
        }
    }
}
