/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.Series;

public class GetSeriesTask implements Callable<List<Series>>
{
    private static Logger log = LoggerFactory.getLogger( GetSeriesTask.class );

    private List<Metric> metrics;
    int from;
    int until;
    int now;
    DataPointStore pointStore;

    public GetSeriesTask(DataPointStore dpStore, List<Metric> metrics, int from, int until, int now)
    {
        this.pointStore = dpStore;
        this.metrics = metrics;
        this.from = from;
        this.until = until;
        this.now = now;

    }
    @Override
    public List<Series> call()
                    throws Exception
    {
        List<Series> results = new ArrayList<>(  );
        for(Metric m : metrics)
        {
            try
            {
                results.add( pointStore.getSeries( m, from, until, now ) );
            }
            catch(Throwable t)
            {
                log.error("Error: ", t );
                Throwables.propagate( t );
            }
        }

        return results;
    }
}
