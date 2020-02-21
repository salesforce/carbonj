/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.List;

import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.IntervalValues;
import com.demandware.carbonj.service.db.model.Intervals;

class IntervalProcessorTaskFactoryImpl implements IntervalProcessorTaskFactory {
    final private DataPointStore pointStore;

    public IntervalProcessorTaskFactoryImpl(DataPointStore pointStore )
    {
        this.pointStore = pointStore;
    }

    @Override
    public IntervalProcessorTask create(List<IntervalValues> intervals)
    {
        return new IntervalProcessorTask( pointStore, new Intervals(intervals) );
    }
}
