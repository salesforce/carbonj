/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;
import java.util.OptionalDouble;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.engine.DataPoint;
import com.google.common.base.Preconditions;

/**
 * Collection of values that belong to the same metric and same interval.
 */
public class IntervalValues
{
    private static final Logger log = LoggerFactory.getLogger( IntervalValues.class );

    public final Metric metric;
    public final int intervalStart;
    public final String dbName;
    public final List<Double> values;

    public IntervalValues(Metric metric, List<Double> values, int intervalStart, String dbName)
    {
        this.metric = Preconditions.checkNotNull( metric );
        this.intervalStart = intervalStart;
        this.values = Preconditions.checkNotNull(values);
        this.dbName = Preconditions.checkNotNull( dbName );
    }

    public DataPoint toDataPoint()
    {
        if( !metric.isLeaf() )
        {
            log.error(String.format( "Metric with id: [%s], name: [%s] is not a leaf metric.", metric.id, metric.name ));
            return null;
        }

        AggregationPolicy aggr = metric.getAggregationPolicy();
        if( aggr == null )
        {
            log.error(String.format("Metric with id: [%s], name: [%s] is missing AggregationPolicy.", metric.id, metric.name));
            return null;
        }

        OptionalDouble od = aggr.apply( values );
        if( od.isPresent() )
        {
            DataPoint p = new DataPoint( metric.name, od.getAsDouble(), intervalStart );
            p.metricId = metric.id;
            return p;
        }
        else
        {
            return null;
        }
    }

    @Override
    public String toString()
    {
        return "IntervalValues{" +
            "metric=" + metric +
            ", intervalStart=" + intervalStart +
            ", dbName='" + dbName + '\'' +
            ", values=" + values +
            '}';
    }
}
