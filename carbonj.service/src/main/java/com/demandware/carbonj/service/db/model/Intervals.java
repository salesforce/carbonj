/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.google.common.base.Preconditions;

/**
 * Set of intervals.
 */
public class Intervals
{
    private static final Logger log = LoggerFactory.getLogger( Intervals.class );

    private List<IntervalValues> intervals;


    public Intervals( List<IntervalValues> batch )
    {
        this.intervals = Preconditions.checkNotNull( batch );
    }

    private int size()
    {
        return intervals.size();
    }

    public DataPoints toDataPoints()
    {
        int n = size();
        DataPoints points = new DataPoints( n );
        for ( int i = 0; i < n; i++ )
        {
            addAggregatedPoint( points, i, intervals.get( i ) );
        }
        return points;
    }

    private void addAggregatedPoint( DataPoints points, int i, IntervalValues iv )
    {
        DataPoint p = iv.toDataPoint();
        if ( p == null )
        {
            log.error( String.format( "Aggregate data point is null for interval [%s]", iv ) );
        }
        points.set( i, p );
        points.assignMetric( i, iv.metric, RetentionPolicy.getInstanceForDbName( iv.dbName ) );
    }
}
