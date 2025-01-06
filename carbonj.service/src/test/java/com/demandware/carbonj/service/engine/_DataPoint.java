/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class _DataPoint
{
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

        int current = (int) (System.currentTimeMillis() / 1000);
        DataPoint dataPoint = new DataPoint("foo.bar", 1, current);
        assertNotEquals(0, dataPoint.hashCode());
        assertTrue(dataPoint.equals(dataPoint));
        assertFalse(dataPoint.equals(null));
        assertFalse(dataPoint.equals(new Object()));
        assertFalse(new DataPoint(null, 1, current, false).equals(dataPoint));
        assertFalse(dataPoint.equals(new DataPoint("foo.bar2", 1, current)));
        assertFalse(dataPoint.equals(new DataPoint("foo.bar", 1, current + 1)));
        assertTrue(dataPoint.equals(new DataPoint("foo.bar", 1, current)));
    }
}
