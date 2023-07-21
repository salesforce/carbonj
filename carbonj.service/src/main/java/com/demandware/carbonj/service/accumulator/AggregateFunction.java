/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.demandware.carbonj.service.engine.DataPoint;

import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;

interface AggregateFunction
{
    enum Type {
        SINGLE_VALUE,
        MULTI_VALUE
    }

    double apply();

    AggregateFunction add( DataPoint v, int now );

    static AggregateFunction create( String key, MetricAggregationMethod method)
    {
        switch (method)
        {
            case AVG:
                return new AvgAggregateFunction();
            case SUM:
                log.info("calling the sum function");
                return new SumAggregateFunction();
            case CUSTOM1:
                return custom1( key );
            case LATENCY:
                return new LatencyAggregateFunction();
            default:
                throw new RuntimeException("Unsupported aggregation method: " + method);

        }
    }

    static AggregateFunction custom1(String key)
    {
        if ( key.endsWith( "mean" ) || key.endsWith( "p95" ) || key.endsWith( "min" ) || key.endsWith( "max" ) )
        {
            return new AvgAggregateFunction();
        }
        return new SumAggregateFunction();
    }

    default Type getType() {
        return Type.SINGLE_VALUE;
    }

    default Map<String,Double> getValues() {
        throw new UnsupportedOperationException();
    }

    static class SumAggregateFunction
        implements AggregateFunction
    {
        double sum = 0;

        @Override
        public synchronized AggregateFunction add( DataPoint dataPoint, int now )
        {
            sum += dataPoint.val;
            log.info("Calculating the sum for aggreggate function");
            log.info("Print the datapoint name: " + dataPoint.name);
            return this;
        }

        @Override
        public synchronized double apply()
        {
            return sum;
        }
    }

    static class AvgAggregateFunction
        extends SumAggregateFunction
    {
        int count = 0;

        @Override
        public synchronized AggregateFunction add( DataPoint dataPoint, int now )
        {
            super.add( dataPoint, now );
            count++;
            return this;
        }

        @Override
        public synchronized double apply()
        {
            if ( 0 == count )
            {
                return 0;
            }
            return sum / count;
        }
    }

    static class LatencyAggregateFunction implements AggregateFunction {

        private final IntSummaryStatistics stats;

        public LatencyAggregateFunction() {
            stats = new IntSummaryStatistics();
        }

        @Override
        public double apply() {
            return 0;
        }

        @Override
        public Map<String, Double> getValues() {
            Map<String, Double> statToValue = new HashMap<>();
            statToValue.put("min", (double) stats.getMin());
            statToValue.put("max", (double) stats.getMax());
            statToValue.put("mean", stats.getAverage());
            statToValue.put("count", (double) stats.getCount());
            return statToValue;
        }

        @Override
        public AggregateFunction add(DataPoint dataPoint, int now) {
            stats.accept(now - dataPoint.ts);
            return this;
        }

        @Override
        public Type getType() {
            return Type.MULTI_VALUE;
        }
    }
}
