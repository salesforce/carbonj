/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;

import com.demandware.carbonj.service.engine.AggregationMethod;
import com.google.common.base.Objects;

/**
 * Policy for propagating data from higher precision archives to lower precision.
 */
public class AggregationPolicy
{
    /**
     * Each lower-precision archive must be divisible by the next higher-precision archive.  300 sec / 60 sec.
     */
    final private AggregationMethod method;

    final private int configRevision;

    final private StorageAggregationPolicySource instanceFactory;


    public AggregationPolicy( AggregationMethod method, int configRevision, StorageAggregationPolicySource policySource )
    {
        this.method = method;
        this.configRevision = configRevision;
        this.instanceFactory = policySource;
    }

    public boolean configChanged()
    {
        return configRevision != instanceFactory.currentConfigRevision();
    }

    public AggregationPolicy getInstance(String metricName)
    {
        return instanceFactory.policyForMetricName( metricName );
    }

    public AggregationMethod getMethod()
    {
        return method;
    }

    public OptionalDouble apply( List<Double> values)
    {
        DoubleStream doubleStream = values.stream().filter(v -> v != null).mapToDouble( v -> v.doubleValue() );
        return OptionalDouble.of(method.apply(doubleStream ));
    }


    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AggregationPolicy that = (AggregationPolicy) o;
        return configRevision == that.configRevision && method == that.method;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode( method, configRevision );
    }
}
