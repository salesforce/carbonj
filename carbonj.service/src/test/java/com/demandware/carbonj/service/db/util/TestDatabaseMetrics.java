/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDatabaseMetrics {
    @Test
    public void testDatabaseMetrics() {
        MetricRegistry metricRegistry = new MetricRegistry();
        DatabaseMetrics databaseMetrics = new DatabaseMetrics(metricRegistry);
        databaseMetrics.markError();
        assertEquals(1, metricRegistry.getMeters().get("db.errors").getCount());
    }
}
