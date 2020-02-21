/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.io.Files;

public class _CreateNames
{
    File dbDirFile;
    MetricIndex index;
    NameUtils nameUtils;

    @Before
    public void setUp()
    {
        nameUtils = new NameUtils(InternalConfig.getRootEntryKey());
        dbDirFile = Files.createTempDir();
        index = IndexUtils.metricIndex( dbDirFile );
        index.open();
    }

    @After
    public void tearDown()
    {
        if( index != null )
        {
            index.close();
        }

        if( dbDirFile != null )
        {
            dbDirFile.delete();
        }
    }

    @Test
    public void newNameIsAlwaysCreatedAsLeaf() throws Exception
    {
        index.setStrictMode( true );

        // create metric
        Metric aMetric = index.createLeafMetric( "a" );
        assertEquals(true, aMetric.isLeaf());
    }


    @Test
    public void doNotAllowNewNodesForLeaf() throws Exception
    {
        index.setStrictMode( true );
        
        // create metric
        Metric aMetric = index.createLeafMetric( "a.b.c" );
        assertEquals(true, aMetric.isLeaf());

        // 1. verify exception is thrown
        try
        {
            index.createLeafMetric( "a.b.c.d" );
            fail("Expected exception");
        }
        catch(RuntimeException e)
        {
            assertEquals("Cannot create metric with name [a.b.c.d] because [a.b.c] is already a leaf",
                e.getMessage());
        }
        // 2. verify we didn't leave any partial nodes behind
        assertNull(index.getMetric( "a.b.c.d" ));
    }

    @Test
    public void doNotAllowNewNodesForLeaf2() throws Exception
    {
        index.setStrictMode( true );

        // create metric
        Metric aMetric = index.createLeafMetric( "a.b.c" );
        assertEquals(true, aMetric.isLeaf());

        // verify exception is thrown
        try
        {
            index.createLeafMetric( "a.b.c.d.e.f" );
            fail("Expected exception");
        }
        catch(RuntimeException e)
        {
            assertEquals("Cannot create metric with name [a.b.c.d.e.f] because [a.b.c] is already a leaf",
                e.getMessage());
        }

        // 2. verify we didn't leave any partial nodes behind
        assertNull(index.getMetric( "a.b.c.d" ));
        assertNull(index.getMetric( "a.b.c.d.e" ));
        assertNull(index.getMetric( "a.b.c.d.e.f" ));
    }
}
