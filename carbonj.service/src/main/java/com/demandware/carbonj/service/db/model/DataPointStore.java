/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;
import java.util.function.Predicate;

import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;

public interface DataPointStore extends StatsAware
{
    DataPointImportResults importDataPoints( String dbName, List<DataPoint> dataPoint, int maxAllowedImportErrors );

    void insertDataPoints( DataPoints points );

    void importDataPoints( String dbName, DataPoints points );

    long delete( String archive, int ts );

    void delete( List<Metric> m );

    Series getSeries( Metric metric, int from, int until, int now );

    List<DataPointValue> getValues( RetentionPolicy archivePolicy, int metricId, int from, int to );

    void open();

    void close();

    DataPointValue getFirst( RetentionPolicy instanceForDbName, int id, int from, int to );
}
