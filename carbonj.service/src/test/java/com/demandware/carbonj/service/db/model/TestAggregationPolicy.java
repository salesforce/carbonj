/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import com.demandware.carbonj.service.engine.AggregationMethod;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAggregationPolicy {
    @Test
    public void test() {
        File storageConfFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("storage-aggregation.conf")).getFile());
        AggregationPolicy aggregationPolicy = new AggregationPolicy(AggregationMethod.AVG, 0,
                new StorageAggregationPolicySource(new StorageAggregationRulesLoader(storageConfFile)));
        assertTrue(aggregationPolicy.configChanged());
        assertEquals(AggregationMethod.AVG, aggregationPolicy.getInstance("foo.bar").getMethod());
        assertEquals(1.5, aggregationPolicy.apply(List.of(1.0, 2.0)).getAsDouble());
        assertTrue(aggregationPolicy.equals(aggregationPolicy));
        assertFalse(aggregationPolicy.equals(new Object()));
        assertTrue(aggregationPolicy.getInstance("foo.bar").equals(aggregationPolicy.getInstance("foo.bar2")));
    }
}
