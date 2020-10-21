/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.demandware.carbonj.service.accumulator.MetricAggregationPolicy;
import com.google.common.base.Preconditions;

import com.demandware.carbonj.service.strings.StringsCache;

public class Metric
{
    final public static Metric METRIC_NULL = NullMetric.getInstance();

    final public long id;
    final public String name; // metric name
    final public List<RetentionPolicy> retentionPolicies;
    final private List<String> children;
    private int lastTs = 0;

    private AggregationPolicy aggregationPolicy;

    private volatile MetricAggregationPolicy metricAggregationPolicy;


    public Metric( String name, long id,
                   AggregationPolicy aggregationPolicy, List<RetentionPolicy> retentionPolicies, List<String> children)
    {
        this.id = id;
        this.name = StringsCache.get(name);
        this.metricAggregationPolicy = null;
        this.aggregationPolicy = aggregationPolicy;
        this.retentionPolicies = retentionPolicies;
        // There will be a lot of Metric instances. Avoid keeping duplicate empty lists around.
        if( children == null || children.size() == 0)
        {
            this.children = Collections.EMPTY_LIST;
        }
        else
        {
            this.children = new ArrayList<>( children.size() );
            for(String s : children)
            {
                this.children.add( StringsCache.get( s ) );
            }
        }
    }

    public MetricAggregationPolicy getMetricAggregationPolicy()
    {
        return metricAggregationPolicy;
    }

    public MetricAggregationPolicy setMetricAggregates(MetricAggregationPolicy metricAggregationPolicy)
    {
        this.metricAggregationPolicy = metricAggregationPolicy;
        return this.metricAggregationPolicy;
    }

    public synchronized boolean isDuplicatePoint(int ts)
    {
        if( lastTs == ts )
        {
            return true;
        }
        else
        {
            lastTs = ts;
            return false;
        }
    }

    public List<String> children()
    {
        return children;
    }

    public Optional<RetentionPolicy> retentionPolicyAfter(RetentionPolicy policy)
    {
        if( policy == null && retentionPolicies.size() > 0)
        {
            return Optional.of(retentionPolicies.get( 0 ));
        }

        for(RetentionPolicy rp : retentionPolicies)
        {
            if( rp.precision > policy.precision)
            {
               return Optional.of( rp );
            }
        }
        return Optional.empty();
    }

    public Optional<RetentionPolicy> retentionPolicyBefore(RetentionPolicy policy)
    {
        Preconditions.checkNotNull( policy );
        Preconditions.checkState( retentionPolicies != null );

        if( retentionPolicies.size() < 2 )
        {
            return Optional.empty();
        }

        RetentionPolicy last = retentionPolicies.get( 0 );
        for(int i = 1; i < retentionPolicies.size(); i++)
        {
            RetentionPolicy rp = retentionPolicies.get( i );
            if( rp.name.equals( policy.name ) )
            {
                return Optional.of( last );
            }
            last = rp;
        }

        return Optional.empty();
    }

    public RetentionPolicy retentionPolicyForDb(String dbName)
    {
        return retentionPolicies.stream().filter( p -> p.dbName.equalsIgnoreCase( dbName ) ).findFirst()
                        .orElseThrow( () -> new RuntimeException(
                                        String.format( "Unknown db name [%s] for metric (id: [%s], name: [%s])",
                                                        dbName, id, name) ));
    }

    public int getMaxRetention()
    {
        if( isLeaf() )
        {
            return getRetentionPolicies().stream().mapToInt( ( t ) -> t.retention ).max()
                            .orElseThrow( () -> new IllegalStateException( "Failed to determine maxRetention" ) );
        }
        else
        {
            return 0;
        }
    }

    public Interval getMaxRetentionInterval(int now)
    {
        if( isLeaf() )
        {
            return new Interval( now - getMaxRetention(), now );
        }
        else
        {
            return new Interval( 0, 0 );
        }
    }

    public List<RetentionPolicy> getRetentionPolicies()
    {
        return retentionPolicies;
    }



    public Optional<RetentionPolicy> getHighestPrecisionArchive()
    {
        if( retentionPolicies != null )
        {
            return retentionPolicies.stream().reduce( ( a, b ) -> RetentionPolicy.higherPrecision( a, b ) );
        }
        else
        {
            return Optional.empty();
        }
    }

    public Optional<RetentionPolicy> pickArchiveForQuery(int from, int until, int now)
    {
        if( retentionPolicies != null )
        {
            return retentionPolicies.stream().filter( p -> p.includes( from, now ) && p.includes( until, now ) )
                            .reduce( ( a, b ) -> RetentionPolicy.higherPrecision( a, b ) );
        }
        else
        {
            return Optional.empty();
        }
    }

    // there shouldn't be many threads trying to perform this operation at the same time on the same metric instance
    public synchronized AggregationPolicy getAggregationPolicy()
    {
        if( aggregationPolicy.configChanged() )
        {
            // using old instance as a factory to create a new one.
            aggregationPolicy = aggregationPolicy.getInstance( name );
        }

        return aggregationPolicy;
    }

    public boolean isLeaf() // identifies nodes that can have data points attached.
    {
        // number of children is not reliable - nonLeaf node can have 0 children after DELETE operation.
        return retentionPolicies != null && retentionPolicies.size() > 0;
        // another option to determine node that can have data points is to test for non-zero id. But long term we might change.
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !( o instanceof Metric ) )
            return false;

        Metric metric = (Metric) o;

        return name.equals( metric.name );

    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        return "Metric{" +
            "id=" + id +
            ", name='" + name + '\'' +
            '}';
    }
}

/*
Sample queries:

Query 1:
curl "http://refapp:8080/metrics/find/?local=1&format=pickle&query=carbon.agents.refapp_mon_demandware_net-a.creates"
Query 2:
curl "http://refapp:8080/metrics/find/?local=1&format=pickle&query=carbon.agents.*.*"
Query 3:
curl "http://refapp:8080/metrics/find/?_dc=1451499994291&query=*&format=treejson&contexts=1&path=&node=GraphiteTree"
GET /metrics/find/?local=1&format=pickle&query=%2A

Response 1:
[{'intervals': [(1419909343.112688, 1451445326.476329)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.creates'}]

Response 2:
[{'intervals': [(1419913385.967088, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.avgUpdateTime'},
 {'intervals': [],
  'isLeaf': False,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.cache'},
 {'intervals': [(1419913385.967141, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.committedPoints'},
 {'intervals': [(1419913385.967171, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.cpuUsage'},
 {'intervals': [(1419913385.967199, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.creates'},
 {'intervals': [(1419913385.967225, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.errors'},
 {'intervals': [(1419913385.967249, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.memUsage'},
 {'intervals': [(1419913385.967273, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.metricsReceived'},
 {'intervals': [(1419913385.967297, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.pointsPerUpdate'},
 {'intervals': [(1419913385.967322, 1451449345.8663266)],
  'isLeaf': True,
  'metric_path': 'carbon.agents.refapp_mon_demandware_net-a.updateOperations'}]
*/
