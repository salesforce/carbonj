/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLatencyAggregation {

    @Test
    public void testLatencyAggregation() {
        int time = TimeSource.systemTimeSource.getEpochSecond();
        AggregateFunction.LatencyAggregateFunction la = new AggregateFunction.LatencyAggregateFunction();
        la.add(new DataPoint("metric1", 2, time), time);
        la.add(new DataPoint("metric2", 2, time), time + 1);
        la.add(new DataPoint("metric3", 2, time), time + 2);
        la.add(new DataPoint("metric4", 2, time), time + 3);
        la.add(new DataPoint("metric5", 2, time), time + 4);
        Map<String, Double> aggTypeToValues = la.getValues();
        assertEquals(4, aggTypeToValues.size());
        assertEquals(0, aggTypeToValues.get("min").intValue());
        assertEquals(4, aggTypeToValues.get("max").intValue());
        assertEquals(5, aggTypeToValues.get("count").intValue());
        assertEquals(2, aggTypeToValues.get("mean").intValue());
    }
}
