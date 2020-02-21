/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.List;

import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.engine.DataPoints;

interface DataPointArchive
{

    String getName();

    void put( int metricId, int time, double val );

    int put( DataPoints points );

    List<Double> getDataPoints( int metricId, int startTime, int endTime, int step );

    List<DataPointValue> getDataPoints( int metricId, int startTime, int endTime );

    void close();

    void open();

    void dumpStats();

    long delete( int ts );

    void deleteMetric( int metricId );

    void deleteMetric( int metricId, int from, int until );

    DataPointValue getFirst( int metricId, int from, int to );
}
