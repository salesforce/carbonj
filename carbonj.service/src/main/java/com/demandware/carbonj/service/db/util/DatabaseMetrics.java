/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class DatabaseMetrics
{
    public static Meter pointsSaved;
    public static Meter pointsRead;
    public static Meter metricsSaved;
    public static Meter errors;
    public static Meter nonLeafMetricsReceived;
    public static Meter invalidLeafMetricsReceived;
    public static Meter queriesServed;

    public static Meter obsoleteSeriesAccessMeter;

    // total time spent on processing getSeries request
    public static Timer getSeriesTimer;

    public static Timer findMetricsTimer;
    public static Histogram seriesPerRequest;
    public static Timer getSeriesTaskReadTimer;
    public static Timer getSeriesTaskSendTimer;
    public static Timer seriesStreamingQueueBlockingTimer;
    public static Timer heavyQueryQueueBlockingTimer;

    public static Meter getSeriesTasksPerQuery;

    public static Timer getSeriesTaskWaitTime;
    public static Timer getSeriesTaskExecutionTime;
    public static Meter getSeriesTaskSize;
    public static Meter getSeriesErrors;
    public static Meter getSeriesTaskReadErrors;
    public static Meter getSeriesTaskSendErrors;
    public static Meter getSeriesWaitForTasksErrors;

    // tracks total time spent on series read operations across all tasks created for one getSeries request
    public static Timer getSeriesReadTimer;
    // tracks total time spent on serialization of data before sending across all tasks created for one getSeries request
    public static Timer getSeriesSerializeTimer;
    // tracks total time spent on all send operations performed across all tasks created for one getSeries request
    public static Timer getSeriesSendTimer;
    // tracks time spent on a single send operation
    public static Timer getSeriesSendOpTimer;
    // tracks number of metrics deleted in delete operation
    public static Meter deletedMetrics;

    public static Meter deletedMetricAccessError;

    // counts how many queries exceeded max datapoints threshold
    public static Meter datapointsLimitExceeded;

    public DatabaseMetrics( MetricRegistry metricRegistry)
    {
        pointsSaved = metricRegistry.meter( MetricRegistry.name( "db", "pointsSaved" ) );
        pointsRead = metricRegistry.meter( MetricRegistry.name( "db", "pointsRead" ) );
        metricsSaved = metricRegistry.meter( MetricRegistry.name( "db", "metricsSaved" ) );
        errors = metricRegistry.meter( MetricRegistry.name( "db", "errors" ) );
        nonLeafMetricsReceived = metricRegistry.meter( MetricRegistry.name( "db", "nonLeafMetricsReceived" ) );
        invalidLeafMetricsReceived = metricRegistry.meter( MetricRegistry.name( "db", "invalidLeafMetricsReceived" ) );
        deletedMetrics = metricRegistry.meter( MetricRegistry.name( "db", "deletedMetrics" ) );
        deletedMetricAccessError = metricRegistry.meter( MetricRegistry.name( "db", "deletedMetricAccessError" ) );
        queriesServed = metricRegistry.meter( MetricRegistry.name( "db", "queriesServed" ) );
        obsoleteSeriesAccessMeter = metricRegistry.meter( MetricRegistry.name( "db", "obsoleteSeriesAccess" ) );
        getSeriesTimer = metricRegistry.timer( MetricRegistry.name("db", "getSeries") );
        findMetricsTimer = metricRegistry.timer( MetricRegistry.name("db", "findMetrics") );
        seriesPerRequest = metricRegistry.histogram( MetricRegistry.name("db", "seriesPerRequest") );
        getSeriesTaskReadTimer = metricRegistry.timer(MetricRegistry.name("db", "getSeriesTaskReadTimer") );
        getSeriesTaskSendTimer = metricRegistry.timer(MetricRegistry.name("db", "getSeriesTaskSendTimer") );
        seriesStreamingQueueBlockingTimer = metricRegistry.timer( MetricRegistry.name("seriesStreamRequestQueue", "taskExecutorBlocks") );
        heavyQueryQueueBlockingTimer = metricRegistry.timer( MetricRegistry.name("heavyQueryQueue", "taskExecutorBlocks") );
        getSeriesTasksPerQuery = metricRegistry.meter( MetricRegistry.name( "db", "getSeriesTasksPerQuery" ) );
        getSeriesTaskWaitTime = metricRegistry.timer(MetricRegistry.name("db", "getSeriesTaskWaitTime"));
        getSeriesTaskExecutionTime = metricRegistry.timer(MetricRegistry.name("db", "getSeriesTaskExecutionTime"));
        getSeriesTaskSize = metricRegistry.meter(MetricRegistry.name("db", "getSeriesTaskSize"));
        getSeriesErrors = metricRegistry.meter(MetricRegistry.name("db", "getSeriesErrors"));
        getSeriesTaskReadErrors = metricRegistry.meter(MetricRegistry.name("db", "getSeriesTaskReadErrors"));
        getSeriesTaskSendErrors = metricRegistry.meter(MetricRegistry.name("db", "getSeriesTaskSendErrors"));
        getSeriesWaitForTasksErrors = metricRegistry.meter(MetricRegistry.name("db", "getSeriesWaitForTasksErrors"));
        getSeriesReadTimer = metricRegistry.timer(MetricRegistry.name("db", "getSeriesReadTimer") );
        getSeriesSerializeTimer = metricRegistry.timer(MetricRegistry.name("db", "getSeriesSerializeTimer") );
        getSeriesSendTimer = metricRegistry.timer(MetricRegistry.name("db", "getSeriesSendTimer") );
        getSeriesSendOpTimer = metricRegistry.timer( MetricRegistry.name("db", "getSeriesSendOpTimer") );
        datapointsLimitExceeded = metricRegistry.meter( MetricRegistry.name( "db", "datapointsLimitExceeded" ) );

    }

    public void markError()
    {
        errors.mark();
    }

    public void markMetricsSaved()
    {
        metricsSaved.mark();
    }

    public void markPointsSaved(int n)
    {
        pointsSaved.mark(n);
    }

    public void markQueriesServed()
    {
        queriesServed.mark();
    }
}
