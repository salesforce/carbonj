/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.model.DeleteAPIResult;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.RetentionPolicyConf;
import com.demandware.carbonj.service.db.model.StorageAggregationPolicySource;
import com.demandware.carbonj.service.db.model.TooManyMetricsFoundException;
import com.demandware.carbonj.service.db.util.CacheStatsReporter;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.strings.StringsCache;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.demandware.carbonj.service.db.index.QueryUtils.filter;
import static com.demandware.carbonj.service.db.index.QueryUtils.splitQuery;

/**
 * Index to access metric metadata. Uses one global lock to perform updates (addition of a new metric). If insert
 * performance will become a concern we can use multiple indexes. We can assign individual metric name to a specific
 * index by applying hash function to top level part of the name.
 */
public class MetricIndexImpl implements MetricIndex {

    private static final String DEFAULT_MAX_SERIES_PER_REQUEST = "30000";
    private static final String DEFAULT_ENFORCE_MAX_SERIES_PER_REQUEST = "true";

    private static final Logger log = LoggerFactory.getLogger( MetricIndexImpl.class );

    private final MetricRegistry metricRegistry;

    private final IndexStore<String, NameRecord> nameIndex;

    private final Map<String, Counter> nameIndexStorePropertyMetricMap = new ConcurrentHashMap<>();

    private final IndexStore<Long, IdRecord> idIndex;

    private final Map<String, Counter> idIndexStorePropertyMetricMap = new ConcurrentHashMap<>();

    private final DatabaseMetrics dbMetrics;

    final private String rootKey = InternalConfig.getRootEntryKey();

    final private long rootId = InternalConfig.getRootEntryId();

    private final RetentionPolicyConf retentionPolicyConf;

    private final Timer insertNewMetricIdTimer;

    private final Random rnd = new Random();

    private AtomicLong nextMetricId = null;

    private final Gauge<Long> lastAssignedMetricIdGauge;

    private final CacheStatsReporter metricCacheStatsReporter;
    private CacheStatsReporter metricIdCacheStatsReporter;
    private final CacheStatsReporter queryCacheStatsReporter;
    private final LoadingCache<String, Metric> metricCache;
    private LoadingCache<Long, Metric> metricIdCache;
    private final LoadingCache<String, List<Metric>> queryCache;

    private final String metricsStoreConfigFile;
    private final NameUtils nameUtils;

    private volatile boolean strictMode = false;

    private final StorageAggregationPolicySource aggrPolicySource;

    /**
     * Maximum number of leaf metric names that can be returned as part of metric query.
     */
    private volatile int maxSeriesPerRequest;

    /**
     * If set to false only warning message will be logged with actual number of metrics matched.
     */
    private volatile boolean enforceMaxSeriesPerRequest;

    private volatile String retentions;

    private final boolean longId;

    private final Counter longIdMetricCounter;

    private final boolean rocksdbReadonly;

    private final boolean syncSecondaryDb;

    private final ConcurrentLinkedQueue<String> nameIndexQueue = new ConcurrentLinkedQueue<>();

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private static class DeleteResult extends DeleteAPIResult
    {
        public List<Metric> metrics = new ArrayList<>();
        public boolean deleteBranch = true;
    }

    public MetricIndexImpl( MetricRegistry metricRegistry, String metricsStoreConfigFile,
                            IndexStore<String, NameRecord> nameIndex, IndexStore<Long, IdRecord> idIndex,
                            DatabaseMetrics dbMetrics, int nameIndexMaxCacheSize, int expireAfterAccessInMinutes,
                            NameUtils nameUtils, StorageAggregationPolicySource aggrPolicySource,
                            int nameIndexQueryCacheMaxSize, int expireAfterWriteQueryCacheInSeconds,
                            boolean idCacheEnabled,
                            boolean longId) {
        this(metricRegistry, metricsStoreConfigFile, nameIndex, idIndex, dbMetrics, nameIndexMaxCacheSize,
                expireAfterAccessInMinutes, nameUtils, aggrPolicySource, nameIndexQueryCacheMaxSize,
                expireAfterWriteQueryCacheInSeconds, idCacheEnabled, longId, false, false);
    }

    public MetricIndexImpl( MetricRegistry metricRegistry, String metricsStoreConfigFile,
                            IndexStore<String, NameRecord> nameIndex, IndexStore<Long, IdRecord> idIndex,
                            DatabaseMetrics dbMetrics, int nameIndexMaxCacheSize, int expireAfterAccessInMinutes,
                            NameUtils nameUtils, StorageAggregationPolicySource aggrPolicySource,
                            int nameIndexQueryCacheMaxSize, int expireAfterWriteQueryCacheInSeconds,
                            boolean idCacheEnabled,
                            boolean longId,
                            boolean rocksdbReadonly,
                            boolean syncSecondaryDb) {
        this.metricRegistry = metricRegistry;
        this.metricsStoreConfigFile = metricsStoreConfigFile;
        this.nameUtils = Preconditions.checkNotNull(nameUtils);
        this.nameIndex = Preconditions.checkNotNull(nameIndex);
        this.idIndex = Preconditions.checkNotNull(idIndex);
        this.dbMetrics = Preconditions.checkNotNull(dbMetrics);
        this.insertNewMetricIdTimer = metricRegistry.timer("db.index.insertNewMetricId.time");
        this.lastAssignedMetricIdGauge = registerLastAssignedMetricIdGauge();
        this.longId = longId;
        this.longIdMetricCounter = metricRegistry.counter("metrics.store.longId");
        loadFromConfigFile(metricsStoreConfigFile, metricRegistry);
        this.retentionPolicyConf = new RetentionPolicyConf(retentions);
        this.rocksdbReadonly = rocksdbReadonly;
        this.syncSecondaryDb = syncSecondaryDb;

        this.metricCache =
                CacheBuilder.newBuilder().initialCapacity(nameIndexMaxCacheSize).maximumSize(nameIndexMaxCacheSize)
                        .recordStats()
                        // TODO: make configurable
                        .concurrencyLevel(8)
                        // TODO: make configurable
                        .expireAfterAccess(expireAfterAccessInMinutes, TimeUnit.MINUTES)
                        .build(new CacheLoader<>() {
                            @SuppressWarnings("NullableProblems")
                            @Override
                            public Metric load(String name) {
                                NameRecord e = nameIndex.dbGet(name);
                                if (e != null) {
                                    return toMetric(e);
                                } else {
                                    return Metric.METRIC_NULL;
                                }
                            }
                        });

        if ( idCacheEnabled )
        {
            this.metricIdCache =
                    CacheBuilder.newBuilder().initialCapacity(nameIndexMaxCacheSize).maximumSize(nameIndexMaxCacheSize)
                            .recordStats()
                            .concurrencyLevel(8)
                            .expireAfterAccess(expireAfterAccessInMinutes, TimeUnit.MINUTES)
                            .build(new CacheLoader<>() {
                                @SuppressWarnings("NullableProblems")
                                @Override
                                public Metric load(Long id)
                                        throws Exception {
                                    IdRecord e = idIndex.dbGet(id);
                                    if (e != null) {
                                        return getMetric(e.metricName());
                                    } else {
                                        return Metric.METRIC_NULL;
                                    }
                                }
                            });

            this.metricIdCacheStatsReporter = new CacheStatsReporter( metricRegistry, "MetricsIdCache", nameIndexMaxCacheSize, metricIdCache );
        }

        this.metricCacheStatsReporter = new CacheStatsReporter( metricRegistry,  "MetricsCache", nameIndexMaxCacheSize, metricCache );


        this.queryCache =
                CacheBuilder.newBuilder()
                        .initialCapacity(nameIndexQueryCacheMaxSize)
                        .maximumSize(nameIndexQueryCacheMaxSize)
                        .recordStats()
                        .concurrencyLevel(8)
                        .expireAfterWrite(expireAfterWriteQueryCacheInSeconds, TimeUnit.SECONDS)
                        .build(new CacheLoader<>() {
                            @SuppressWarnings("NullableProblems")
                            @Override
                            public List<Metric> load(String pattern) {
                                try {
                                    return findMetricsNoCache(pattern, true, true, true);
                                } catch (TooManyMetricsFoundException e) {
                                    log.error("Error: ", e);
                                    return Collections.emptyList();
                                }
                            }
                        });
        this.queryCacheStatsReporter = new CacheStatsReporter( metricRegistry, "MetricsQueryCache", nameIndexQueryCacheMaxSize, queryCache );

        this.aggrPolicySource = Preconditions.checkNotNull( aggrPolicySource );

        if (syncSecondaryDb) {
            scheduledExecutorService.scheduleAtFixedRate(
                    new SyncSecondaryDbTask(nameIndex.getDbDir()), 60, 30, TimeUnit.SECONDS);
        } else if (rocksdbReadonly) {
            scheduledExecutorService.scheduleAtFixedRate(
                    new SyncNameIndexCacheTask(nameIndex.getDbDir()), 60, 30, TimeUnit.SECONDS);
        }
    }

    private void loadFromConfigFile(String metricsStoreConfigFileName, MetricRegistry metricRegistry) {
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

        enforceMaxSeriesPerRequest = Boolean.parseBoolean(
                properties.getProperty("metrics.store.enforceMaxSeriesPerRequest", DEFAULT_ENFORCE_MAX_SERIES_PER_REQUEST));
        maxSeriesPerRequest = Integer.parseInt(
                properties.getProperty("metrics.store.maxSeriesPerRequest", DEFAULT_MAX_SERIES_PER_REQUEST));
        retentions = properties.getProperty("metrics.store.retentions", RetentionPolicyConf.DEFAULT_RETENTIONS);
        String dbPropertiesValue = properties.getProperty("metrics.store.dbProperties");
        if (!StringUtils.isEmpty(dbPropertiesValue)) {
            parseDbProperties(dbPropertiesValue, metricRegistry);
        }
    }

    void parseDbProperties(String dbProperties, MetricRegistry metricRegistry) {
        for (String dbProperty : StringUtils.split(dbProperties, ',')) {
            if (!nameIndexStorePropertyMetricMap.containsKey(dbProperty)) {
                nameIndexStorePropertyMetricMap.put(dbProperty, new Counter());
                if (metricRegistry != null) {
                    metricRegistry.register("db." + nameIndex.getName() + "." + dbProperty,
                            nameIndexStorePropertyMetricMap.get(dbProperty));
                }
            }
            if (!idIndexStorePropertyMetricMap.containsKey(dbProperty)) {
                idIndexStorePropertyMetricMap.put(dbProperty, new Counter());
                if (metricRegistry != null) {
                    metricRegistry.register("db." + idIndex.getName() + "." + dbProperty,
                            idIndexStorePropertyMetricMap.get(dbProperty));
                }
            }
        }
    }

    @Override
    public void setStrictMode( boolean mode )
    {
        this.strictMode = mode;
    }

    @Override
    public boolean isStrictMode()
    {
        return this.strictMode;
    }

    @Override
    public boolean isValidName( String name )
    {
        return nameUtils.isValid( name, true );
    }

    @Override
    public void dumpIndex( File dumpFile )
    {
        try
        {
            PrintWriter pw = new PrintWriter( dumpFile );
            pw.println( "*** NAME INDEX START ***" );
            nameIndex.dump( pw );
            pw.println( "*** NAME INDEX END ***" );

            pw.println( "*** ID INDEX START ***" );
            idIndex.dump( pw );
            pw.println( "*** ID INDEX END ***" );
            pw.close();
        }
        catch ( IOException e )
        {
            Throwables.throwIfUnchecked(e);
        }
    }

    @Override
    public Metric getMetric( long metricId )
    {
        if( metricIdCache == null )
        {
            IdRecord e = idIndex.dbGet(metricId);
            if (e != null)
            {
                return getMetric(e.metricName());
            }
            else
            {
                return null;
            }
        }
        else
        {
            try {
                Metric m = metricIdCache.get(metricId);
                return m == Metric.METRIC_NULL ? null : m;
            } catch (ExecutionException e) {
                Throwables.throwIfUnchecked(e);
                return null;
            }
        }
    }

    @Override
    public String getMetricName( long metricId )
    {
        IdRecord e = idIndex.dbGet(metricId);
        if (e != null)
        {
            return e.metricName();
        }
        else
        {
            return null;
        }
    }

    @Override
    public Metric forId( long metricId )
    {
        return getMetric( metricId );
    }

    @Override
    public void dumpStats()
    {
        Snapshot snapshot = insertNewMetricIdTimer.getSnapshot();
        log.info( String.format( "new metric insert: mean=%s, max=%s, min=%s, 95p=%s", snapshot.getMean(),
            snapshot.getMax(), snapshot.getMin(), snapshot.get95thPercentile() ) );
        log.info( String.format( "metric name index: nextMetricId=%s", lastAssignedMetricIdGauge.getValue() ) );
        nameIndex.dumpStats();
        idIndex.dumpStats();
        metricCacheStatsReporter.dumpStats();
        if( metricIdCacheStatsReporter != null )
        {
            metricIdCacheStatsReporter.dumpStats();
        }
        queryCacheStatsReporter.dumpStats();
        dumpDbPropertyStats(nameIndexStorePropertyMetricMap, nameIndex);
        dumpDbPropertyStats(idIndexStorePropertyMetricMap, idIndex);
    }

    private <K> void dumpDbPropertyStats(Map<String, Counter> indexStorePropertyMetricMap,
                                         IndexStore<K, ? extends Record<K>> indexStore) {
        Set<String> dbProperties = new HashSet<>(indexStorePropertyMetricMap.keySet());
        for (String property : dbProperties) {
            String value = indexStore.dbGetProperty(property);
            if (!StringUtils.isEmpty(value)) {
                indexStorePropertyMetricMap.get(property).inc(Long.parseLong(value));
            }
        }
    }

    Map<String, Counter> getNameIndexStorePropertyMetricMap() {
        return ImmutableMap.copyOf(nameIndexStorePropertyMetricMap);
    }

    Map<String, Counter> getIdIndexStorePropertyMetricMap() {
        return ImmutableMap.copyOf(idIndexStorePropertyMetricMap);
    }

    @Override
    public void open()
    {
        nameIndex.open();
        idIndex.open();
        createRootIfMissing();
        log.info( "Loading maxMetricId..." );
        long maxId = findMaxMetricId(); // should not be null because root node is inserted first if index is empty.
        log.info( "maxMetricId=" + maxId );
        setMaxId(maxId);
        log.info("Long Id support: " +  longId );
        if(longId)
        {
            longIdMetricCounter.inc();
        }
    }

    @Override
    public void setMaxId(long maxId)
    {
        this.nextMetricId = new AtomicLong( maxId + 1);
        log.info( "nextMetricId=" + this.nextMetricId );
    }

    private long findMaxMetricId()
    {
        return idIndex.maxKey();
    }

    @Override
    public void close()
    {
        nameIndex.close();
        idIndex.close();
        unregisterMetricIdGauge();
        metricCacheStatsReporter.close();
        if( metricIdCacheStatsReporter != null )
        {
            metricIdCacheStatsReporter.close();
        }
        queryCacheStatsReporter.close();
        if (syncSecondaryDb || rocksdbReadonly) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Override
    public Metric getMetric( String key )
    {
        try
        {
            Metric m = metricCache.get( key );
            if ( m == Metric.METRIC_NULL)
            {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Metric is null for the id [%s]", key));
                }
                return null;
            }
            return m;
        }
        catch ( ExecutionException e )
        {
            Throwables.throwIfUnchecked(e);
            return null;
        }
    }

    private void createRootIfMissing()
    {
        Metric root = getMetric( rootKey );
        if ( root == null )
        {
            NameRecord nr = new NameRecord( rootKey, rootId, false );
            nameIndex.dbPut( nr );
            updateCache( nr );
            idIndex.dbPut( new IdRecord( rootId, rootKey ) );
        }
    }

    @Override
    public List<Metric> deleteMetric( String name, boolean recursive, boolean testRun )
    {
        Optional<String> parentName = nameUtils.parentName( name );

        // root lock will be obtained additionally for the duration of root update.
        // During insert locks are obtained in the same order: 1. namespace lock, 2. root lock only if necessary.
        Object lock = namespaceLock( name );

        DeleteResult deleteResult = new DeleteResult();
        synchronized ( lock )
        {
            doDeleteMetric( deleteResult, parentName.orElse( rootKey ), name, recursive, testRun, false, Collections.emptySet() );
        }
        return  deleteResult.metrics;
    }

    @Override
    public DeleteAPIResult deleteAPI(String name, boolean delete, Set<String> exclude)
    {
        List<String> metricNames = new ArrayList<>();
        // id starting with **. is considered as segment delete. Ex: **.order.count
        if( name.startsWith("**.") )
        {
            metricNames.addAll( findAllMetricsWithSegment(rootKey, 0, splitQuery(name.split("\\.", 2)[1] ))) ;
        }
        else
        {
            metricNames.addAll( findMetrics( rootKey, 0, splitQuery( name ), false, Integer.MAX_VALUE, false )
                    .stream().map(m -> m.name ).toList());
        }

        DeleteResult deleteResult = new DeleteResult();
        for ( String metricName : metricNames )
        {
            Optional<String> parentName = nameUtils.parentName( metricName );
            Object lock = namespaceLock( metricName );

            synchronized ( lock )
            {
                doDeleteMetric( deleteResult, parentName.orElse( rootKey ), metricName, true, delete, true, exclude );
            }
        }
        deleteResult.setMetricsList(metricNames);
        return deleteResult;
    }

    private boolean doDeleteMetric(DeleteResult deleteResult, String parentName,
                                        String name,
                                        boolean recursive,
                                        boolean testRun,
                                        boolean countOnly,
                                        Set<String> exclude)
    {
        Metric m = getMetric( name );
        if ( m == null )
        {
            // we are not supposed to be here. Looks like we are in bad state. Lets remove the metric from parent.
            deleteMetricAndRemoveFromParent(parentName, name);

            if ( strictMode )
            {
                throw new RuntimeException( String.format( "Metric [%s] not found.", name ) );
            }
            else
            {
                log.error( String.format( "Metric [%s] not found.", name ) );
                return true;
            }
        }

        if ( !m.isLeaf() && !recursive )
        {
            throw new RuntimeException( String.format(
                "Cannot delete non-leaf metric [%s] when recursive flag is false.", name ) );
        }

        if( !countOnly )
        {
            deleteResult.metrics.add( m );
        }

        boolean deleteBranch = true;
        for ( String child : m.children() )
        {
            // Dont delete the child branch if the child is in exclude list
            if(exclude.contains(child))
            {
                deleteBranch = false;
                continue;
            }
            deleteBranch = doDeleteMetric( deleteResult, m.name, nameUtils.toMetricName( m.name, child ), recursive, testRun, countOnly, exclude ) && deleteBranch;
        }

        if ( deleteBranch )
        {
            if( !testRun )
            {
                deleteMetricAndRemoveFromParent(parentName, name);
            }
            if( countOnly )
            {
                deleteResult.setTotalCount(deleteResult.getTotalCount() + 1);
                if(m.isLeaf()){
                    deleteResult.setLeafCount( deleteResult.getLeafCount() + 1);
                }
            }
        }
        return  deleteBranch;
    }

    private void deleteMetricAndRemoveFromParent(String parentName, String name ) {
        if ( parentName != null )
        {
            if ( nameUtils.isTopLevel( name ) )
            {
                synchronized ( rootKey )
                {
                    deleteChild( parentName, name );
                }
            }
            else
            {
                // already synchronized on the whole namespace
                deleteChild( parentName, name );
            }
        }
    }

    @Override
    public List<String> getTopLevelNames()
    {
        return getChildNames( rootKey );
    }

    @Override
    public List<String> getChildNames( String metricName )
    {
        Preconditions.checkNotNull( metricName );

        Metric parent = getMetric( metricName );

        return new ArrayList<>( parent.children() );
    }

    @Override
    public List<Metric> findMetrics( String pattern )
    {
        return findMetrics(rootKey, 0, splitQuery( pattern ), false, Integer.MAX_VALUE, false );
    }


    /**
     * Loads metrics with names matching the pattern. Non-leaf metrics and metrics with invalid names are excluded.
     *
     * @param pattern to search for
     * @return list of matched metrics. Empty list is returned if none found or number of matches exceeds specified
     * threshold.
     */
    @Override
    public List<Metric> findMetrics(String pattern, boolean leafOnly, boolean useThreshold, boolean skipInvalid)
    {
        // use cache only in these cases
        if( leafOnly && useThreshold && skipInvalid )
        {
            try
            {
                List<Metric> metrics = queryCache.get(pattern);
                if (rocksdbReadonly && metrics.isEmpty()) {
                    queryCache.invalidate(pattern);
                }
                return metrics;
            }
            catch(ExecutionException e)
            {
                Throwables.throwIfUnchecked(e);
                return null;
            }
        }
        else
        {
            return findMetricsNoCache(pattern, leafOnly, useThreshold, skipInvalid);
        }
    }

    private List<Metric> findMetricsNoCache(String pattern, boolean leafOnly, boolean useThreshold, boolean skipInvalid)
    {
        List<Metric> metrics;
        int threshold = enforceMaxSeriesPerRequest && useThreshold ? maxSeriesPerRequest : Integer.MAX_VALUE;

        try
        {
            metrics = findMetrics(rootKey, 0, splitQuery(pattern), leafOnly, threshold, skipInvalid);
            DatabaseMetrics.seriesPerRequest.update( metrics.size() );
        }
        catch(TooManyMetricsFoundException e)
        {
            DatabaseMetrics.seriesPerRequest.update( threshold );
            String msg = String.format("Name pattern resolves to too many metric names." +
                    "Pattern [%s], configured threshold value: %s", pattern, threshold );
            log.warn(msg);
            throw new TooManyMetricsFoundException(e.getLimit(), msg);
        }

        // can happen if enforceMaxSeriesPerRequest is false
        if ( metrics.size() > maxSeriesPerRequest )
        {
            log.warn( "Name pattern resolves to too many metric names. Pattern [" + pattern
                    + "], matched metric names: " + metrics.size() + ", configured threshold value: "
                    + maxSeriesPerRequest );
        }

        return metrics;
    }

    private List<Metric> findMetrics( String parentKey, int queryPartIdx, String[] queryParts, boolean leafOnly, int max,
                                      boolean excludeInvalid )
    {
        Metric parent = getMetric( parentKey );
        if ( parent == null )
        {
            DatabaseMetrics.deletedMetricAccessError.mark();
            return Collections.emptyList();
        }
        List<String> matches = filter( parent.children(), queryParts[queryPartIdx] );
        boolean isLastQuerySegment = queryPartIdx + 1 >= queryParts.length;
        if ( isLastQuerySegment )
        {
            return matches.stream().map( childName -> toMetricName( parentKey, childName ) )
                .filter(childKey -> !excludeInvalid || isValidName(childKey))
            // .peek( System.out::println )
                .map(this::getMetric)
                // childKey was present in parent but not found. This means inconsistent data.
                // if strictMode == false cover up and filter out such children otherwise include null values in the
                // result.
                .filter( m -> strictMode || m != null)
                .filter( m -> !leafOnly || m != null && m.isLeaf())
                .collect( Collectors.toList() );
        }
        else
        {
            List<Metric> matchedMetrics = new ArrayList<>();
            for ( String childName : matches )
            {
                String childKey = toMetricName( parentKey, childName );
                try
                {
                    matchedMetrics.addAll( findMetrics( childKey, queryPartIdx + 1, queryParts, leafOnly, max, excludeInvalid ) );
                }
                catch( TooManyMetricsFoundException e)
                {
                    Throwables.throwIfUnchecked(e);
                }
                catch ( Throwable t )
                {
                    if ( strictMode )
                    {
                        Throwables.throwIfUnchecked(t);
                    }
                    else
                    {
                        log.error( String.format( "Failed to find metrics for [%s], [%s], [%s], [%s]", childKey,
                            queryPartIdx + 1, Arrays.toString( queryParts ), leafOnly ), t );
                    }
                }
                if( matchedMetrics.size() > max )
                {
                    throw new TooManyMetricsFoundException(max);
                }
            }
            return matchedMetrics;
        }

    }


    private List<String> findMetricWithSegment(String parentKey, int queryPartIdx, String[] queryParts)
    {
        Metric parent = getMetric( parentKey );
        List<String> matched = new ArrayList<>();
        for ( String childName: parent.children() )
        {
            String childKey = toMetricName( parentKey, childName );
            int qIndex = queryPartIdx;
            if(childName.equals(queryParts[queryPartIdx]))
            {
                boolean isLastSegment = queryPartIdx + 1 >= queryParts.length;
                if( isLastSegment )
                {
                    Metric child = getMetric( childKey );
                    matched.add(child.name);
                    continue;
                }
                else
                {
                    qIndex++;
                }
            }
            else
            {
                qIndex = queryPartIdx != 0 ? (childName.equals(queryParts[0])? 1 : 0)  : queryPartIdx;
            }
            matched.addAll(findMetricWithSegment(childKey, qIndex, queryParts));
        }
        return matched;
    }
    private List<String> findAllMetricsWithSegment( String parentKey, int queryPartIdx, String[] queryParts )
    {
        return new ArrayList<>(findMetricWithSegment(parentKey, queryPartIdx, queryParts));
    }

    private String toMetricName( String parent, String child )
    {
        return nameUtils.toMetricName( parent, child );
    }

    @Override
    public Metric selectRandomMetric()
    {
        int maxAttempts = 10;
        int attempts = 0;
        String pattern = "*";

        while ( true )
        {
            List<Metric> metrics = findMetrics( pattern );
            if (metrics.isEmpty())
            {
                if ( attempts > maxAttempts )
                {
                    throw new RuntimeException( "Failed to select random metric." );
                }

                attempts = attempts + 1;
                pattern = "*";
            }
            else
            {
                Metric m = metrics.get( rnd.nextInt( metrics.size() ) );

                if ( m.isLeaf() )
                {
                    return m;
                }
                else
                {
                    pattern = m.name + ".*";
                }
            }
        }
    }

    @Override
    public Metric createLeafMetric( String name )
    {
        // TODO: consolidate with validator that checks for max length after that PR is merged to master.
        if ( !nameUtils.isValid( name ) )
        {
            return null;
        }

        Metric m = getMetric( name );
        if ( m == null )
        {
            Object namespaceLock = namespaceLock( name );
            synchronized ( namespaceLock )
            {
                m = getMetric( name );
                if ( m == null )
                {
                    m = insert( name );
                }
                else
                {
                    // TODO: for now just log. Eventually switch to throwing an exception.
                    if ( !m.isLeaf() )
                    {
                        log.error( String.format( "Invalid name index state. Expected a leaf metric for name [%s]",
                            name ) );
                    }
                }
            }
        }
        return m;
    }

    private Object namespaceLock( String name )
    {
        return StringsCache.get( nameUtils.firstSegment( name ) );
    }

    private Metric toMetric( NameRecord r )
    {
        String key = r.getKey();
        return new Metric( key, r.getId(), aggrPolicySource.policyForMetricName( key ),
            r.getRetentionPolicies(), r.getChildren() );
    }

    private long nextMetricId()
    {
        // Need this int value conversion when longId = false. Without this cache and store values
        // reflect different ids when the id goes over Integer.MAX_VALUE.
        // Example: for the value 2147483649 without conversion, store will have id -2147483647 and cache will have 2147483649
        return longId ? this.nextMetricId.getAndIncrement() : ((Long)this.nextMetricId.getAndIncrement()).intValue();
    }

    private String[] pathsForMetricName( String name )
    {
        String[] names = nameUtils.metricNameHierarchy( name );
        return ObjectArrays.concat( rootKey, names );
    }

    private Metric insert( String key )
    {
        final Timer.Context timerContext = insertNewMetricIdTimer.time();
        // TODO: probably redundant. get rid of it.
        dbMetrics.markMetricsSaved();
        try
        {
            // TODO: add more logic to detect cases if tree is not in the state we expected:
            // For example: non-leaf node has non-zero id value.
            String[] paths = pathsForMetricName( key );

            NameRecord leafEntry = insertLeaf( key );
            for ( int i = paths.length - 2; i >= 0; i-- )
            {
                String entryKey = paths[i];
                NameRecord e = nameIndex.dbGet( entryKey );
                if ( e != null )
                {
                    if ( e.isLeaf() ) // this case should be infrequent.
                    {
                        DatabaseMetrics.invalidLeafMetricsReceived.mark();
                        cleanupAfterAbandonedInsert( leafEntry, paths, i + 1 );
                        String msg =
                            String.format( "Cannot create metric with name [%s] because [%s] is already a leaf", key,
                                entryKey );
                        throw new RuntimeException( msg );
                    }
                    // key already exists. may need to update list of children
                    updateNonLeaf( e, paths[i + 1] );
                    // done. parent node exists and up-to-date.
                    break;
                }
                else
                // this key doesn't exist
                {
                    // create a new non-leaf entry for the key
                    createNoneLeaf( entryKey, paths[i + 1] );
                }
            }

            return updateCache( leafEntry );
        }
        finally
        {
            timerContext.stop();
        }
    }

    private void cleanupAfterAbandonedInsert( NameRecord leafEntry, String[] paths, int from )
    {
        // delete entries that we've just inserted
        deleteFromIdIndexQuietly( leafEntry.getId() );
        for ( int i = from; i < paths.length; i++ )
        {
            deleteFromNameIndexQuietly( paths[i] );
        }
    }

    private void deleteFromNameIndexQuietly( String dbKey )
    {
        if (log.isDebugEnabled())
        {
            log.debug( "removing key: " + dbKey );
        }

        try
        {
            nameIndex.dbDelete( dbKey );
            metricCache.invalidate( dbKey );
        }
        catch ( Exception e )
        {
            log.error( String.format( "Failed to delete key from the name index. key [%s]", dbKey ), e );
        }

    }

    private void deleteFromIdIndexQuietly( long dbKey )
    {
        try
        {
            idIndex.dbDelete( dbKey );
            if( metricIdCache != null )
            {
                metricIdCache.invalidate(dbKey);
            }
        }
        catch ( Exception e )
        {
            log.error( String.format( "Failed to delete key from the id index. key [%s]", dbKey ), e );
        }

    }

    private Metric updateCache( NameRecord e )
    {
        Metric m = toMetric( e );
        metricCache.put( m.name, m );
        if( metricIdCache != null )
        {
            metricIdCache.put(m.id, m);
        }
        if (syncSecondaryDb) {
            nameIndexQueue.offer(m.name);
        }
        return m;
    }

    private NameRecord insertLeaf( String key )
    {
        long metricId = nextMetricId();
        NameRecord leafEntry = new NameRecord( key, metricId, true );
        leafEntry.setRetentionPolicies( getRetentionPolicies( key ) );

        IdRecord idIndexEntry = new IdRecord( metricId, key );

        nameIndex.dbPut( leafEntry );
        idIndex.dbPut( idIndexEntry );
        return leafEntry;
    }

    private boolean updateNonLeaf( NameRecord e, String childKey )
    {
        if ( e.addChildKeyIfMissing( childKey ) )
        {
            if ( rootKey.equals( e.getKey() ) )
            {
                return updateRootEntry( childKey );
            }
            else
            {
                // save because list of children was updated.
                nameIndex.dbPut( e );
                updateCache( e );
                return true;
            }
        }
        else
        {
            return false;
        }
    }

    private boolean updateRootEntry( String childKey )
    {
        synchronized ( rootKey )
        {
            NameRecord e = nameIndex.dbGet( rootKey );
            if ( e.addChildKeyIfMissing( childKey ) )
            {
                nameIndex.dbPut( e );
                updateCache( e );
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    private void deleteMetric( String name ) {
        Metric m = getMetric( name );
        nameIndex.dbDelete( name );
        if ( m.id > 0 )
        {
            idIndex.dbDelete( m.id );
        }
        metricCache.invalidate( m.name );
        if( metricIdCache != null )
        {
            metricIdCache.invalidate(m.id);
        }
        DatabaseMetrics.deletedMetrics.mark();
    }

    private boolean deleteChild( String parentKey, String childKey )
    {

        // delete child and remove from the parent
        deleteMetric( childKey );

        NameRecord e = nameIndex.dbGet( parentKey );

        if ( e == null )
        {
            String msg =
                String.format( "Cannot delete child [%s] from parent [%s] because parent does not exist.", childKey,
                    parentKey );
            if ( strictMode )
            {
                throw new RuntimeException( msg );
            }
            else
            {
                log.error( msg );
                return false;
            }
        }

        if ( e.removeChildKeyIfExists( childKey ) )
        {
            // save because list of children was updated.
            nameIndex.dbPut( e );
            updateCache( e );
            return true;
        }
        else
        {
            return false;
        }
    }

    private NameRecord createNoneLeaf( String key, String childKey )
    {
        NameRecord e = new NameRecord( key, 0, false );
        e.addChildKeyIfMissing( childKey );
        nameIndex.dbPut( e );
        updateCache( e );
        return e;
    }

    private List<RetentionPolicy> getRetentionPolicies( String metricName )
    {
        return retentionPolicyConf.getRetentionPolicies( metricName );
    }

    String maxMetricIdGaugeName()
    {
        return MetricRegistry.name( "db.index", "maxMetricId" );
    }

    private void unregisterMetricIdGauge()
    {
        metricRegistry.remove( maxMetricIdGaugeName() );
    }

    private Gauge<Long> registerLastAssignedMetricIdGauge()
    {
        return metricRegistry.register( maxMetricIdGaugeName(),
                this::lastAssignedMetricIdGaugeValue);
    }

    private long lastAssignedMetricIdGaugeValue()
    {
        return nextMetricId.get();
    }

    @Override
    public long scanNames( long start, long end, Consumer<Metric> c )
    {
        return idIndex.scan( start, end, r -> c.accept( getMetric( r.metricName() ) ) );
    }

    public void reload() {
        loadFromConfigFile(metricsStoreConfigFile, metricRegistry);
    }

    private class SyncSecondaryDbTask implements Runnable {
        private final File syncSecondaryDbDir;

        private SyncSecondaryDbTask(File dbDir) {
            this.syncSecondaryDbDir = new File(dbDir.getParentFile(), dbDir.getName() + "-sync");
        }

        @Override
        public void run() {
            if (nameIndexQueue.isEmpty()) {
                return;
            }
            int size = nameIndexQueue.size();
            List<String> nameIndexes = new ArrayList<>();
            while (size-- > 0) {
                nameIndexes.add(nameIndexQueue.poll());
            }
            if (!syncSecondaryDbDir.exists()) {
                log.info("Creating directory {}", syncSecondaryDbDir.getAbsolutePath());
                if (!syncSecondaryDbDir.mkdir()) {
                    log.error("Failed to create directory {}", syncSecondaryDbDir.getAbsolutePath());
                    logError(nameIndexes);
                    return;
                }
            }
            if (!syncSecondaryDbDir.canWrite()) {
                log.error("Cannot write to directory {}", syncSecondaryDbDir.getAbsolutePath());
                logError(nameIndexes);
                return;
            }
            File syncFile = new File(syncSecondaryDbDir, "sync-" + Clock.systemUTC().millis());
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(syncFile, StandardCharsets.UTF_8))) {
                for (String nameIndex : nameIndexes) {
                    bufferedWriter.write(nameIndex);
                    bufferedWriter.newLine();
                }
            } catch (IOException e) {
                log.error("Failed to sync name indexes to {} - {}", syncFile.getAbsolutePath(), e.getMessage(), e);
                logError(nameIndexes);
            }
        }

        private void logError(List<String> nameIndexes) {
            log.error("Unable to update name indexes {}", StringUtils.join(nameIndexes, ','));
        }
    }

    private class SyncNameIndexCacheTask implements Runnable {
        private final File syncSecondaryDbDir;

        private SyncNameIndexCacheTask(File dbDir) {
            this.syncSecondaryDbDir = new File(dbDir.getParentFile(), dbDir.getName() + "-sync");
        }

        @Override
        public void run() {
            if (!syncSecondaryDbDir.exists()) {
                log.error("Directory {} does not exist", syncSecondaryDbDir.getAbsolutePath());
                return;
            }
            if (!syncSecondaryDbDir.canWrite()) {
                log.error("Cannot read from directory {}", syncSecondaryDbDir.getAbsolutePath());
                return;
            }
            File[] syncFiles = syncSecondaryDbDir.listFiles((dir, name) -> name.startsWith("sync-"));
            if (syncFiles == null || syncFiles.length == 0) {
                log.info("No name index file to sync");
                return;
            }
            int size = nameIndexQueue.size();
            while (size-- > 0) {
                String nameIndex = nameIndexQueue.poll();
                if (nameIndex != null) {
                    refreshNameIndex(nameIndex);
                }
            }
            for (File syncFile : syncFiles) {
                TreeSet<String> nameIndexes = new TreeSet<>();
                try (BufferedReader bufferedReader = new BufferedReader(new FileReader(syncFile))) {
                    log.info("Syncing name indexes from {}", syncFile.getAbsolutePath());
                    String nameIndex;
                    while ((nameIndex = bufferedReader.readLine()) != null) {
                        nameIndex = nameIndex.trim();
                        int index = nameIndex.lastIndexOf('.');
                        while (index > 0) {
                            nameIndexes.add(nameIndex);
                            nameIndex = nameIndex.substring(0, index);
                            index = nameIndex.lastIndexOf('.');
                        }
                        nameIndexes.add(nameIndex);
                    }
                } catch (IOException e) {
                    log.error("Failed to sync name indexes from {} - {}", syncFile.getAbsolutePath(), e.getMessage(), e);
                    continue;
                }
                if (!syncFile.delete()) {
                    log.error("Failed to delete {}", syncFile.getAbsolutePath());
                }
                for (String nameIndex : nameIndexes) {
                    refreshNameIndex(nameIndex);
                }
            }
        }

        private void refreshNameIndex(String nameIndex) {
            log.info("Refreshing cache for name index {}", nameIndex);
            metricCache.invalidate(nameIndex);
            if (getMetric(nameIndex) == null) {
                nameIndexQueue.offer(nameIndex);
            }
        }
    }
}
