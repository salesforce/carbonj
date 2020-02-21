/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.db.model.IntervalValues;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Takes sorted staging file and generates aggregated data points for appropriate archive (database).
 */
class IntervalProcessors
{
    private static final Logger log = LoggerFactory.getLogger( IntervalProcessors.class );

    private final MetricRegistry metricRegistry;
    private IntervalProcessorTaskFactory taskFactory;
    private final int emptyQueuePauseInMillis;

    private ConcurrentMap<String, IntervalProcessor> map = new ConcurrentHashMap<>(  );

    final private int batchSizePerTask;
    final private int queueSizePerDb;
    final private int nConsumersPerDb;

    public IntervalProcessors(MetricRegistry metricRegistry, int batchSizePerTask, int queueSizePerDb, int nConsumersPerDb,
                              IntervalProcessorTaskFactory taskFactory, int emptyQueuePauseInMillis)
    {
        this.metricRegistry = metricRegistry;
        this.batchSizePerTask = batchSizePerTask;
        this.queueSizePerDb = queueSizePerDb;
        this.nConsumersPerDb = nConsumersPerDb;
        this.taskFactory = taskFactory;
        this.emptyQueuePauseInMillis = emptyQueuePauseInMillis;
    }

    // synchronized to make sure that we don't end up with more than one running processor for the same dbName
    private synchronized IntervalProcessor intervalProcessorForDbName(String dbName)
    {
        return map.computeIfAbsent( dbName, key -> startNewIntervalProcessor( key ) );
    }

    private IntervalProcessor startNewIntervalProcessor(String dbName)
    {
        IntervalProcessor p = new IntervalProcessorImpl(metricRegistry,  dbName, batchSizePerTask, queueSizePerDb, nConsumersPerDb,
                taskFactory, emptyQueuePauseInMillis );
        new Thread(p, dbName + "-interval-processor").start();
        return p;
    }

    public void shutdown()
    {
        map.values().forEach( v -> v.close() );
    }

    public Stats processFile(SortedStagingFile stagingFile)
    {
        log.info( "processing sorted file: [" + stagingFile + "]" );
        long start = System.currentTimeMillis();
        try
        {
            String dbName = stagingFile.dbName();
            IntervalProcessor processor = intervalProcessorForDbName( dbName );
            Preconditions.checkState(stagingFile.isClosed());
            stagingFile.open();
            int nRecords = 0;
            int nLines = 0;
            Optional<IntervalValues> ons;
            while( true )
            {
                try
                {
                    ons = stagingFile.loadNeighbours();
                }
                catch(RuntimeException e)
                {
                    log.error( "Failed to load points from staging file.", e );
                    continue;
                }

                if( !ons.isPresent() ) // eof
                {
                    break;
                }

                IntervalValues ns = ons.get();
                nRecords++;
                nLines = nLines + ns.values.size();
                processor.put( ns );
            }

            long time = System.currentTimeMillis() - start;
            log.info( String.format( "completed processing sorted file: [%s] for dbName [%s] with [%s] original points aggregated into [%s] points in [%s] ms",
                stagingFile, dbName, nLines, nRecords, time));
            return new Stats(nLines, nRecords, time, dbName, stagingFile);
        }
        catch(Throwable t)
        {
            log.error( "Unhandled error when processing staging file: [" + stagingFile + "]", t );
            // Don't attempt processing the rest of the files.
            // Recoverable errors at individual metric level should not be propagated to this level.
            Throwables.propagate( t );
            return null;
        }
        finally
        {
            stagingFile.close();
        }
    }

    static class Stats {
        int nLines;
        int nRecords;
        long time;
        String dbName;
        SortedStagingFile sortedStagingFile;

        public Stats(int nLines, int nRecords, long time, String dbName,
                                           SortedStagingFile sortedStagingFile) {
            this.nLines = nLines;
            this.nRecords = nRecords;
            this.time = time;
            this.dbName = dbName;
            this.sortedStagingFile = sortedStagingFile;
        }
    }
}
