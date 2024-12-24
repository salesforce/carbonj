/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import com.demandware.carbonj.service.engine.AggregationMethod;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestIntervals {
    @Test
    public void testIntervals() {
        File storageConfFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("storage-aggregation.conf")).getFile());
        StorageAggregationPolicySource storageAggregationPolicySource = new StorageAggregationPolicySource(
                new StorageAggregationRulesLoader(storageConfFile));
        AggregationPolicy aggregationPolicy1 = new AggregationPolicy(AggregationMethod.AVG, 1, storageAggregationPolicySource);
        Metric metric1 = new Metric("foo.bar1", 1, aggregationPolicy1, List.of(RetentionPolicy.getInstance("60s:24h")), new ArrayList<>());
        IntervalValues intervalValues1 = new IntervalValues(metric1, List.of(1.0, 2.0), 0, "60s24h");
        AggregationPolicy aggregationPolicy2 = new AggregationPolicy(AggregationMethod.SUM, 1, storageAggregationPolicySource);
        Metric metric2 = new Metric("foo.bar2", 2, aggregationPolicy2, List.of(RetentionPolicy.getInstance("60s:24h")), new ArrayList<>());
        IntervalValues intervalValues2 = new IntervalValues(metric2, List.of(3.0, 4.0), 0, "60s24h");
        Intervals intervals = new Intervals(List.of(intervalValues1, intervalValues2));
        DataPoints dataPoints = intervals.toDataPoints();
        assertEquals(2, dataPoints.size());
        assertEquals("foo.bar1", dataPoints.get(0).name);
        assertEquals("foo.bar2", dataPoints.get(1).name);
    }
}
