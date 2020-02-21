/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.util.Arrays;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.google.common.base.Preconditions;

@Deprecated
public class MetricIndexTask
    implements Runnable
{
    static Meter leavesFailed;

    static Meter leavesCreated;

    private final MetricIndex index;

    private final TimeSeriesStore store;

    private final DataPoint dp;

    public MetricIndexTask(MetricRegistry metricRegistry, TimeSeriesStore store, MetricIndex index, DataPoint dp )
    {
        this.index = Preconditions.checkNotNull( index );
        this.store = Preconditions.checkNotNull( store );
        this.dp = Preconditions.checkNotNull( dp );
        leavesFailed = metricRegistry.meter(
                MetricRegistry.name( "asyncMetricIndexTasks", "leavesFailed" ) );
        leavesCreated = metricRegistry.meter(
                MetricRegistry.name( "asyncMetricIndexTasks", "leavesCreated" ) );
    }

    @Override
    public String toString()
    {
        return "MetricIndexTask{" + dp + '}';
    }

    @Override
    public void run()
    {
        if ( index.createLeafMetric( dp.name ) != null )
        {
            leavesCreated.mark();
            store.accept( new DataPoints( Arrays.asList( dp ) ) );
        }
        else
        {
            leavesFailed.mark();
        }
    }
}
