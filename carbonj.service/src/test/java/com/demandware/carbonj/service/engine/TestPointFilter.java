/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.index.NameUtils;
import com.demandware.carbonj.service.db.util.Quota;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPointFilter {
    @Test
    public void testPointFilter() {
        MetricRegistry metricRegistry = new MetricRegistry();
        PointFilter pointFilter = new PointFilter(metricRegistry, "pointFilter", 1, 1, 1, new NameUtils(), 1, 1, new Quota(100, 1));
        int current = (int) (System.currentTimeMillis() / 1000);
        DataPoint dataPoint = new DataPoint("", 1, current);
        assertFalse(pointFilter.accept(dataPoint));
        dataPoint = new DataPoint("foo", 1, current);
        assertFalse(pointFilter.accept(dataPoint));
        dataPoint = new DataPoint("f", 1, current - 2);
        assertFalse(pointFilter.accept(dataPoint));
        dataPoint = new DataPoint("f", 2, current + 2);
        assertFalse(pointFilter.accept(dataPoint));
        dataPoint = new DataPoint("f", 1, current);
        assertTrue(pointFilter.accept(dataPoint));
        assertFalse(pointFilter.accept(dataPoint));
    }
}
