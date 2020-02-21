/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.Query;
import com.demandware.carbonj.service.engine.ResponseStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Metric;

class StreamSeriesBatchMetricsTask
    implements Callable<BatchStats>
{
    final private static Logger log = LoggerFactory.getLogger( StreamSeriesBatchMetricsTask.class );

    final private Query query;

    final private List<Metric> metrics;

    final private DataPointStore pointStore;

    final private ResponseStream r;

    final private QueryDurations durations;

    final private long taskCreated = System.nanoTime();

    StreamSeriesBatchMetricsTask( DataPointStore dpStore, List<Metric> metrics,
                                         Query query,
                                         ResponseStream r, QueryDurations durations )
    {
        this.pointStore = dpStore;
        this.metrics = metrics;
        this.query = query;
        this.r = r;
        this.durations = durations;
    }

    @Override
    @SuppressWarnings("unused")
    public BatchStats call()
    {

        long waitTimeInNanoSeconds = System.nanoTime() - taskCreated;
        DatabaseMetrics.getSeriesTaskWaitTime.update(waitTimeInNanoSeconds, TimeUnit.NANOSECONDS);
        DatabaseMetrics.getSeriesTaskSize.mark(metrics.size());

        LongSummaryStatistics readTimeStats = new LongSummaryStatistics();
        LongSummaryStatistics emptySeriesReadTimeStats = new LongSummaryStatistics();
        LongSummaryStatistics sendTimeStats = new LongSummaryStatistics();

        int noOfDataPoints = 0;
        try(Timer.Context t = DatabaseMetrics.getSeriesTaskExecutionTime.time())
        {
            ReadResult readResult = read(readTimeStats, emptySeriesReadTimeStats);
            send(readResult.seriesList, sendTimeStats);
            noOfDataPoints += readResult.totalNoOfDataPoints;
        }
        catch(Throwable t)
        {
            log.error("Unexpected exception. " + this, t);
        }

        return new BatchStats(waitTimeInNanoSeconds/1000L, noOfDataPoints, readTimeStats, sendTimeStats, emptySeriesReadTimeStats);
    }

    private ReadResult read(LongSummaryStatistics readTimeStats, LongSummaryStatistics emptySeriesReadTimeStats)
    {
        List<Series> series = new ArrayList<>();
        Timer.Context t = DatabaseMetrics.getSeriesTaskReadTimer.time();
        int noOfDataPoints = 0;
        try
        {
            for (Metric m: metrics) {
                long startTime = System.currentTimeMillis();
                Series result = pointStore.getSeries(m, query.from(), query.until(), query.now());
                long duration =  System.currentTimeMillis() - startTime;
                if (result.values.size() == 0) {
                    emptySeriesReadTimeStats.accept(duration);
                } else {
                    readTimeStats.accept(duration);
                }
                series.add(result);
                noOfDataPoints += result.values.size();
            }
        }
        catch(Throwable e)
        {
            DatabaseMetrics.getSeriesTaskReadErrors.mark();
            throw new RuntimeException("Failed to fetch series.", e);
        }
        finally
        {
            durations.addRead(t.stop());
        }
        return new ReadResult(series, noOfDataPoints);
    }

    private static class ReadResult {
        final List<Series> seriesList;
        final int totalNoOfDataPoints;

        private ReadResult(List<Series> seriesList, int totalNoOfDataPoints) {
            this.seriesList = seriesList;
            this.totalNoOfDataPoints = totalNoOfDataPoints;
        }
    }


    private void send(List<Series> series, LongSummaryStatistics sendTimeStats)
    {
        Timer.Context t = DatabaseMetrics.getSeriesTaskSendTimer.time();
        try
        {
            series.forEach(s -> writeSeriesQuietly(s, sendTimeStats));
        }
        catch(Throwable e)
        {
            DatabaseMetrics.getSeriesTaskSendErrors.mark();
            throw new RuntimeException("Failed to send response.", e);
        }
        finally
        {
            durations.addSerializeAndSend(t.stop());
        }
    }

    private void writeSeriesQuietly(Series s, LongSummaryStatistics sendTimeStats)
    {
        try
        {
            long startTime = System.currentTimeMillis();
            r.writeSeries(s);
            sendTimeStats.accept(System.currentTimeMillis() - startTime);
        }
        catch(IOException e)
        {
            DatabaseMetrics.getSeriesTaskSendErrors.mark();
            log.error( String.format( "Failed to stream series [%s] ", s), e );
        }
    }


    @Override
    public String toString()
    {
        return String.format("Task [%s] for pattern [%s], from=%s, until=%s, now=%s, size=%s",
                hashCode(), query.pattern(), query.from(), query.until(), query.now(), metrics.size() );
    }
}
