/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.demandware.carbonj.service.engine.DataPoint;
import com.google.common.base.Preconditions;

/**
 * Staging queue processor.
 */
public class DataPointStagingStore
                extends Thread
{
    private static final Logger log = LoggerFactory.getLogger( DataPointStagingStore.class );

    private  final MetricRegistry metricRegistry;

    static Meter received;

    static Meter drop;

    static Meter saved;
    
    private final int emptyQueuePauseInMillis;

    private volatile boolean stop = false;

    private ArrayBlockingQueue<StagingFileRecord> queue;

    private ExecutorService intervalProcessorExecutorService;
    private ExecutorCompletionService<IntervalProcessors.Stats> executorCompletionService;

    final private StagingFiles stagingFiles;

    private final StagingFileSetProvider stagingFileSetProvider = new StagingFileSetProvider();

    private final int queueSize;

    private IntervalProcessors intervalProcessors;

    final private int stagingIntervalQueueConsumerBatchSize;
    final private int stagingIntervalsQueueSizePerDb;
    final private int stagingIntervalsQueueConsumersPerDb;
    final private int timeAggrJobIntervalInMins;
    final private int timeAggrJobThreads;

    public DataPointStagingStore(MetricRegistry metricRegistry, StagingFiles stagingFiles,
                                 int stagingQueueSize, int stagingIntervalQueueConsumerBatchSize,
                                 int stagingIntervalsQueueSizePerDb, int stagingIntervalsQueueConsumersPerDb,
                                 int emptyQueuePauseInMillis, int timeAggrJobIntervalInMins, int timeAggrJobThreads)
    {
        this.metricRegistry = metricRegistry;
        this.emptyQueuePauseInMillis = emptyQueuePauseInMillis;
        this.timeAggrJobIntervalInMins = timeAggrJobIntervalInMins;
        this.timeAggrJobThreads = timeAggrJobThreads;
        this.setName( this.getClass().getSimpleName() );
        this.setDaemon( true );
        this.stagingFiles = Preconditions.checkNotNull(stagingFiles);
        this.queueSize = stagingQueueSize;
        this.stagingIntervalQueueConsumerBatchSize = stagingIntervalQueueConsumerBatchSize;
        this.stagingIntervalsQueueSizePerDb = stagingIntervalsQueueSizePerDb;
        this.stagingIntervalsQueueConsumersPerDb = stagingIntervalsQueueConsumersPerDb;

        received = metricRegistry.meter( MetricRegistry.name( "staging", "recv" ) );
        drop = metricRegistry.meter( MetricRegistry.name( "staging", "drop" ) );
        saved = metricRegistry.meter( MetricRegistry.name( "staging", "saved" ) );
    }

    public void open(DataPointStore pointStore)
    {
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        this.queue = new ArrayBlockingQueue<>( queueSize );
        IntervalProcessorTaskFactory taskFactory = new IntervalProcessorTaskFactoryImpl( pointStore);
        this.intervalProcessors = new IntervalProcessors(metricRegistry, stagingIntervalQueueConsumerBatchSize,
            stagingIntervalsQueueSizePerDb, stagingIntervalsQueueConsumersPerDb, taskFactory, emptyQueuePauseInMillis );
        s.scheduleAtFixedRate(this::propagateDb5m7d, 1, timeAggrJobIntervalInMins, TimeUnit.MINUTES );
        s.scheduleAtFixedRate(this::propagateDb30m2y, 1 + timeAggrJobIntervalInMins / 2, timeAggrJobIntervalInMins, TimeUnit.MINUTES );
        s.scheduleWithFixedDelay(this::cleanup, 15, 5, TimeUnit.MINUTES );

        intervalProcessorExecutorService = Executors.newFixedThreadPool(timeAggrJobThreads, new ThreadFactory() {

            private int threadNo = 1;

            @SuppressWarnings("NullableProblems")
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "Interval-Processors-" + (threadNo++));
                thread.setDaemon(true);
                return thread;
            }
        });
        executorCompletionService = new ExecutorCompletionService<>(intervalProcessorExecutorService);

        this.start();
    }

    public void cleanup()
    {
        try
        {
            // delete files older than 120min
            stagingFiles.deleteFilesOlderThan( TimeSource.defaultTimeSource().getEpochSecond() - 60 * 120 );
        }
        catch(Throwable t)
        {
            log.error("Unexpected failure when deleting old staging files.", t );
        }
    }

    public IntervalProcessors.Stats processStagingFile(SortedStagingFile stagingFile)
    {
        return this.intervalProcessors.processFile( stagingFile );
    }

    public synchronized void propagateDb5m7d() {
        propagate("5m7d");
    }

    public synchronized void propagateDb30m2y() {
        propagate("30m2y");
    }

    public void propagate(String dbName)
    {
        log.info( "propagating points from staged files for db {}.", dbName );
        try
        {
            List<Future<IntervalProcessors.Stats>> statsList = stagingFiles.collectEligibleFiles(dbName, this);

            for (Future<IntervalProcessors.Stats> result : statsList) {

                IntervalProcessors.Stats stats = result.get();

                metricRegistry.counter(
                        MetricRegistry.name("staging", "intervalprocessor", stats.dbName, "time")).inc(stats.time);
                metricRegistry.counter(
                        MetricRegistry.name("staging", "intervalprocessor", stats.dbName, "metrics", "raw")).inc(stats.nLines);
                metricRegistry.counter(
                        MetricRegistry.name("staging", "intervalprocessor", stats.dbName, "metrics", "aggr")).inc(stats.nRecords);

                log.info( String.format("finished propagating points from staged files. processed [%s] files.", stats.sortedStagingFile) );
            }
        }
        catch(Throwable t)
        {
            log.error( "Failed to propagate data from staged files.", t );
        }
    }

    public void dumpStats()
    {
        log.info(String.format("Staging store stats: received: %s, dropped: %s, saved: %s",
                        received.getCount(), drop.getCount(), saved.getCount()));
    }

    public void add( String dbName, int from, long metricId, double val, String metricName )
    {
        received.mark();

        StagingFileSet stagingFile = stagingFileSetProvider.get( dbName, from, (int) (metricId % 10));
        StagingFileRecord r = new StagingFileRecord( stagingFile, metricId, DataPoint.strValue(val), metricName );

        if ( queue.offer( r ) ) //TODO: slow down instead of dropping?
        {
            return;
        }
        drop.mark();
        if ( log.isDebugEnabled() )
        {
            log.debug( "Dropped->" + this + ". Queue size " + queue.size() + ". Total dropped " + drop.getCount() );
        }
    }

    @Override
    public void run()
    {
        int batchSize = 10000;
        List<StagingFileRecord> batch = new ArrayList<>( batchSize );
        try
        {
            registerQueueDepthGauge();

            while ( true )
            {
                if ( stop )
                {
                    return;
                }

                try
                {
                    queue.drainTo(batch, batchSize);
                    if(batch.isEmpty())
                    {
                        // no new data. take this opportunity to flush
                        stagingFiles.flush();

                        // check again
                        queue.drainTo(batch, batchSize);
                        if(batch.isEmpty())
                        {
                            // still nothing.
                            TimeUnit.MILLISECONDS.sleep( 100 );
                            continue;
                        }
                    }

                    for ( StagingFileRecord r : batch )
                    {
                        stagingFiles.write( r );
                    }
                    saved.mark( batch.size() );
                    batch.clear();
                }
                catch ( Exception e )
                {
                    log.error( "Failure saving metrics for aggregation.", e );
                }
            }
        }
        catch ( Throwable e )
        {
            log.error( "Unhandled error." + this, e );
        }
        finally
        {
            stagingFiles.close();
            unregisterQueueDepthGauge();
        }
    }

    private String queueSizeGaugeName()
    {
        return MetricRegistry.name( "staging", "queue" );
    }

    private void unregisterQueueDepthGauge()
    {
        metricRegistry.remove( queueSizeGaugeName() );
    }

    private void registerQueueDepthGauge()
    {
        metricRegistry.register( queueSizeGaugeName(), (Gauge<Number>) () -> queue.size() );
    }

    public void closeQuietly()
    {
        try
        {
            close();
        }
        catch ( Exception e )
        {
            log.error( "Failed to close [" + this + "]", e );
        }
    }

    void close()
    {
        log.info( "Stopping " + this );
        stop = true;
        try
        {
            TimeUnit.SECONDS.timedJoin( this, 30 );
            log.info( this + " stopped." );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException(e);
        }

        intervalProcessorExecutorService.shutdown();
        this.intervalProcessors.shutdown();
    }

    public Future<IntervalProcessors.Stats> submitIntervalProcessorTask(SortedStagingFile sortedFile) {
        return this.executorCompletionService.submit(new IntervalProcessorTask(this, sortedFile));
    }

    private static class IntervalProcessorTask implements Callable<IntervalProcessors.Stats> {

        private final DataPointStagingStore stagingStore;
        private final SortedStagingFile sortedStagingFile;

        private IntervalProcessorTask(DataPointStagingStore stagingStore, SortedStagingFile sortedStagingFile) {
            this.stagingStore = stagingStore;
            this.sortedStagingFile = sortedStagingFile;
        }

        @Override
        public IntervalProcessors.Stats call()  {
            return stagingStore.processStagingFile(sortedStagingFile);
        }
    }

}
