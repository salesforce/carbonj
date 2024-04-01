/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class SecondaryRocksDbTest extends BaseIndexTest {
    private MetricIndex metricIndexReadonly;

    @Test
    public void indexNameSyncTest() throws Exception {
        index.createLeafMetric("a.b.c.d");
        index.close();
        metricIndexReadonly = IndexUtils.metricIndexReadonly( dbDirFile, false );
        metricIndexReadonly.open();
        Metric metric = metricIndexReadonly.getMetric("a.b.c.d");
        assertNotNull(metric);
        assertEquals("a.b.c.d", metric.name);

        try {
            metricIndexReadonly.createLeafMetric("a.b.c.e");
            fail("Method createLeafMetric should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException ignored) {
        }
        try {
            metricIndexReadonly.deleteMetric("a.b.c.d", false, false);
            fail("Method createLeafMetric should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException ignored) {
        }
    }

    @After
    public void tearDown() {
        metricIndexReadonly.close();
    }
}
