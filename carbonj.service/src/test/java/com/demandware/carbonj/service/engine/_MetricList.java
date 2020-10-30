/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.cc.infra.core.kinesis.Message;
import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.util.TestFileUtils;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class _MetricList
{
    File configFile;

    MetricList metricList;

    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Before
    public void setUp()
        throws Exception
    {
        configFile = TestFileUtils.setupTestFileFromResource("/metriclist-test.conf");
        metricList = new MetricList( metricRegistry, "test", configFile );
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
}
