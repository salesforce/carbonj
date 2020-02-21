/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.stream.DoubleStream;

/**
 * Method for propagating data from higher precision archives to lower precision archives.
 */
public enum AggregationMethod
{
    NOT_AVAILABLE(null),
    AVG(new Avg()),
    SUM(new Sum()),
    MIN(new Min()),
    MAX(new Max()),
    LAST(new Last());

    final private Aggregator aggr;

    AggregationMethod(Aggregator aggr)
    {
        this.aggr = aggr;
    }

    public double apply( DoubleStream values)
    {
        return aggr.apply( values );
    }

    private static class Avg implements Aggregator
    {
        @Override
        public double apply( DoubleStream values )
        {
            return values.average().getAsDouble();
        }
    }

    private static class Last implements Aggregator
    {
        @Override
        public double apply( DoubleStream values )
        {
            return values.min().getAsDouble();
        }
    }

    private static class Max implements Aggregator
    {
        @Override
        public double apply( DoubleStream values )
        {
            return values.max().getAsDouble();
        }
    }

    private static class Min implements Aggregator
    {
        @Override
        public double apply( DoubleStream values )
        {
            return values.min().getAsDouble();
        }
    }

    private static class Sum implements Aggregator
    {
        @Override
        public double apply( DoubleStream values )
        {
            return values.sum();
        }
    }

    interface Aggregator
    {
        double apply( DoubleStream values);
    }

}

