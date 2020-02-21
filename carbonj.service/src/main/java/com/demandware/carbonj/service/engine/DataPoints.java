/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.RetentionPolicy;

public class DataPoints
{
    private static Logger log = LoggerFactory.getLogger( DataPoints.class );

    private List<DataPoint> points;

    private Metric[] metrics;

    private RetentionPolicy[] pointPolicies;

    private Set<RetentionPolicy> policies;

    public DataPoints( int size )
    {
        this( Arrays.asList( new DataPoint[size] ) );
    }

    public DataPoints( List<DataPoint> points )
    {
        this.points = points;
        this.metrics = new Metric[points.size()];
        this.pointPolicies = new RetentionPolicy[points.size()];
        this.policies = new HashSet<>();
    }

    public int size()
    {
        return points.size();
    }

    public void set( int i, DataPoint p )
    {
        points.set( i, p );
    }

    public DataPoint get( int i )
    {
        return points.get( i );
    }

    public void assignMetric( int i, Metric m, RetentionPolicy policy )
    {
        try
        {
            if ( m == null || policy == null )
            {
                throw new IllegalStateException( String.format( "Point: %s, m: %s, policy: %s", points.get( i ), m, policy ) );
            }

            metrics[i] = m;

            DataPoint p = points.get( i );
            p.setMetricId( m.id );
            pointPolicies[i] = policy;
            policies.add( policy );

        }
        catch(IllegalStateException e)
        {
            log.error( "Invalid state - missing metric or policy", e);
        }
    }

    public void assignMetric( int i, Metric m, Function<Metric, RetentionPolicy> policyResolver )
    {
        assignMetric( i, m, policyResolver.apply( m ) );
    }

    public Metric getMetric( int i )
    {
        return metrics[i];
    }

    public RetentionPolicy getPolicy( int i )
    {
        return pointPolicies[i];
    }

    public Set<RetentionPolicy> getPresentPolicies()
    {
        return policies;
    }

}
