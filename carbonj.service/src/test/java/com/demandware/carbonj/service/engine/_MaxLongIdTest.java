/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.Metric;
import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class _MaxLongIdTest extends CarbonJSvcLongIdTest
{
    private static final long maxLongId = Long.MAX_VALUE;

    @Autowired
    private MetricRegistry metricRegistry;

    @Test
    public void testMaxIntId() {
        metricIndex.setMaxId(maxLongId);
        String ns = "a.b.c.d";
        Metric metric = metricIndex.createLeafMetric(ns);
        assertEquals(maxLongId + 1, metric.id);
        assertEquals(ns, metricIndex.getMetricName(metric.id));
    }

    @Test
    public void testMultipleNameSpaces() {
        metricIndex.setMaxId(maxLongId);
        String ns1 = "a.1";
        String ns2 = "b.2";
        String ns3 = "c.3";
        Metric m1 = metricIndex.createLeafMetric(ns1);
        Metric m2 = metricIndex.createLeafMetric(ns2);
        Metric m3 = metricIndex.createLeafMetric(ns3);

        assertEquals(maxLongId + 1, m1.id);
        assertEquals(maxLongId + 2, m2.id);
        assertEquals(maxLongId + 3, m3.id);
        assertEquals(ns1, metricIndex.getMetricName(m1.id));
        assertEquals(ns2, metricIndex.getMetricName(m2.id));
        assertEquals(ns3, metricIndex.getMetricName(m3.id));
    }

    @Test
    public void testDpsWithMaxIntId() {
        metricIndex.setMaxId(maxLongId);
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "a.2", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "b.1", 2.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "b.2", 3.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();
        assertEquals( dps, cjClient.dumpLines( DB_60S, null, null, 0, Integer.MAX_VALUE ) );
        assertEquals("a.1", metricIndex.getMetricName(maxLongId + 1));
        assertEquals("a.2", metricIndex.getMetricName(maxLongId + 2));
        assertEquals("b.1", metricIndex.getMetricName(maxLongId + 3));
        assertEquals("b.2", metricIndex.getMetricName(maxLongId + 4));
    }

    @Test
    public void testMaxIdWithCrossingIntMax() {
        metricIndex.setMaxId(maxLongId - 2);
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "a.2", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "b.1", 2.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "b.2", 3.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "b.3", 4.0f, DataPoint.align2Min( new DateTime() ) ),
                new DataPoint( "b.4", 4.0f, DataPoint.align2Min( new DateTime() ) ));
        cjClient.send( dps );
        drain();
        assertEquals( dps, cjClient.dumpLines( DB_60S, null, null, 0, Integer.MAX_VALUE ) );
        assertEquals("a.1", metricIndex.getMetricName(maxLongId - 1));
        assertEquals("a.2", metricIndex.getMetricName(maxLongId));
        assertEquals("b.1", metricIndex.getMetricName(maxLongId + 1));
        assertEquals("b.2", metricIndex.getMetricName(maxLongId + 2));
        assertEquals("b.3", metricIndex.getMetricName(maxLongId + 3));
        assertEquals("b.4", metricIndex.getMetricName(maxLongId + 4));
    }

    @Test
    public void testLongIdSupportMetric() {
        assertEquals(1, metricRegistry.counter("metrics.store.longId").getCount());
    }
}
