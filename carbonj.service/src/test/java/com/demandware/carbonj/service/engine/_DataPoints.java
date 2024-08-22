/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class _DataPoints
{

    @Test
    public void canUpdatePointsInTheList()
    {
        int n = 10;
        DataPoints points = new DataPoints( n );
        for(int i = 0; i < n; i++)
        {
            points.set( i, new DataPoint(String.valueOf( i ), 1.0, 1234) );
        }

        for(int i = 0; i < n; i++)
        {
            assertEquals(String.valueOf( i ), points.get( i ).name);
        }
    }
}
