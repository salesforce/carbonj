/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import com.demandware.carbonj.service.engine.AggregationMethod;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestIntervalValues {
    @Test
    public void testIntervalValues() {
        Metric m = new Metric( "a.b", 1, null, new ArrayList<>(), null );
        assertFalse(m.isLeaf());
        IntervalValues intervalValues = new IntervalValues(m, new ArrayList<>(), 0, "60s24h");
        assertNull(intervalValues.toDataPoint());
        List<RetentionPolicy> retentionPolicyList = new ArrayList<>();
        retentionPolicyList.add(RetentionPolicy.getInstance("60s:24h"));
        File storageConfFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("storage-aggregation.conf")).getFile());
        AggregationPolicy aggregationPolicy = new AggregationPolicy(AggregationMethod.AVG, 0,
                new StorageAggregationPolicySource(new StorageAggregationRulesLoader(storageConfFile)));
        m = new Metric( "a.b", 1, aggregationPolicy, retentionPolicyList, null );
        intervalValues = new IntervalValues(m, List.of(1.0, 2.0), 120, "60s24h");
        DataPoint dataPoint = intervalValues.toDataPoint();
        assertNotNull(dataPoint);
        assertEquals("a.b", dataPoint.name);
        assertEquals(120, dataPoint.ts);
        assertEquals(1.5, dataPoint.val);
        assertEquals("IntervalValues{metric=Metric{id=1, name='a.b'}, intervalStart=120, dbName='60s24h', values=[1.0, 2.0]}", intervalValues.toString());
    }
}
