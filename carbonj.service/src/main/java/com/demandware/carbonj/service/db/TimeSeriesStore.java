/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.demandware.carbonj.service.db.model.*;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.engine.Query;
import com.demandware.carbonj.service.engine.ResponseStream;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface TimeSeriesStore
    extends Consumer<DataPoints>, StatsAware
{
    void put( DataPoints point );

    /**
     * @param dbName
     * @param points
     * @param maxImportErrors
     * @return
     * @deprecated use (void TimeSeriesStore.importPoints) instead
     */
    @Deprecated
    DataPointImportResults importPoints( String dbName, List<DataPoint> points, int maxImportErrors );

    void importPoints( String dbName, DataPoints points );

    DataPointExportResults exportPoints( String dbName, String metricName );

    DataPointExportResults exportPoints( String dbName, long metricId );

    // to support testing
    Metric selectRandomMetric();

    Metric getMetric( String name );

    Metric getMetric( String name, boolean createIfMissing );

    Metric getMetric( long metricId );

    String getMetricName( long metricId );

    void scanMetrics( Consumer<Metric> m );

    long scanMetrics( long start, long end, Consumer<Metric> m );

    List<Metric> findMetrics( String pattern );

    List<Metric> findMetrics( String pattern, boolean leafOnly, boolean useThreshold, boolean skipInvalid );

    List<Series> fetchSeriesData( Query query);

    void streamSeriesData(Query query, ResponseStream seriesStream )
        throws IOException;

    List<Metric> deleteMetric( String name, boolean recursive, boolean testRun );

    DeleteAPIResult deleteAPI( String name, boolean delete, Set<String> exclude );

    long deleteDataPoints( String archive, int ts );

    void dumpIndex( File file );

    void deleteAll();

    List<DataPointValue> getValues( String dbName, String metricName, int from, int to );

    DataPointValue getFirst( String dbName, String metricName, int from, int to );

    void drain();
}
