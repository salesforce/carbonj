/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.strings.StringsCache;

import com.demandware.carbonj.service.util.TestFileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class _MetricList
{
    File configFile;

    MetricList metricList;

    private final MetricRegistry metricRegistry = new MetricRegistry();

    @BeforeEach
    public void setUp()
        throws Exception
    {
        configFile = TestFileUtils.setupTestFileFromResource("/metriclist-test.conf");
        metricList = new MetricList( metricRegistry, "test", configFile, "file", null );
    }

    @Test
    public void testMatch()
    {
        assertTrue( metricList.match( "pod11.ecom.a.b.c.min" ) );
    }

    @Test
    public void testNoMatch()
    {
        assertFalse( metricList.match( "pod11.ecom.a.b.c.count" ) );
    }

    public static void main(String[] args) throws Exception {
        File blackListRuleFile = TestFileUtils.setupTestFileFromResource("/blacklist.conf");
        MetricRegistry metricRegistry = new MetricRegistry();
        MetricList metricList = new MetricList( metricRegistry, "test", blackListRuleFile, "file", null );
        List<DataPoint> dataPoints = new ArrayList<>();
        // Comment out the caching to run the test with repeated black list rules checking
        new StringsCache( metricRegistry, 5000000, 10000000, 180, 8 );
        File file = new File("/tmp/audit.txt");
        if (file.exists()) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    DataPoint dataPoint = LineProtocolHandler.parse(line);
                    if (dataPoint != null) {
                        dataPoints.add(dataPoint);
                    }
                }
            }
        }
        long matchedCount = 0L;
        long startCpuTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
        for (DataPoint dataPoint : dataPoints) {
            if (metricList.match(dataPoint.name)) {
                matchedCount++;
            }
        }
        long endCpuTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
        System.out.println("Total = " + dataPoints.size());
        System.out.println("Matched = " + matchedCount);
        System.out.println("CPU time (ms) = " + (endCpuTime - startCpuTime) / 1_000_000);
    }
}
