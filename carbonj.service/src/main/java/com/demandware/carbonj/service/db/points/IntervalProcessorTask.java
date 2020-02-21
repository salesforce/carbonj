/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Intervals;
import com.demandware.carbonj.service.engine.DataPoints;

/**
 * Consumer tasks that receives a batch of intervals to process.
 */
class IntervalProcessorTask implements Runnable
{
    private final Intervals intervals;
    private final DataPointStore pointStore;

    public IntervalProcessorTask( DataPointStore pointStore, Intervals intervals)
    {
        this.pointStore = pointStore;
        this.intervals = intervals;
    }

    @Override
    public void run()
    {
        DataPoints points = intervals.toDataPoints();
        pointStore.insertDataPoints( points );
    }
}
