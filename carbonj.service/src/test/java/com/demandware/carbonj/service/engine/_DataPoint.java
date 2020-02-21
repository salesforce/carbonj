/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class _DataPoint
{
    @Before
    public void setUp()
    {
    }

    @Test
    public void dataPointToString()
    {
        assertEquals( "test 1.99 1", new DataPoint( "test", 1.985, 1 ).toString() );
        assertEquals( "test 1.98 1", new DataPoint( "test", 1.984, 1 ).toString() );
        assertEquals( "test 2 1", new DataPoint( "test", 2.0, 1 ).toString() );
        assertEquals( "test 0.99 1", new DataPoint( "test", 0.991, 1 ).toString() );
        assertEquals( "test 0.01 1", new DataPoint( "test", 0.005, 1 ).toString() );
        assertEquals( "test 0.09 1", new DataPoint( "test", 0.09, 1 ).toString() );
        assertEquals( "test 0.1 1", new DataPoint( "test", 0.1, 1 ).toString() );
        assertEquals( "test 50 1", new DataPoint( "test", 50.0, 1 ).toString() );
        assertEquals( "test 123456 1", new DataPoint( "test", 123456.00, 1 ).toString() );
    }
}
