/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.admin;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.engine.DataPoint;

public class _DumpFormat
{
    @Test
    public void writeSeries()
    {
        Assert.assertEquals( "abc|100|1|2",
            DumpFormat.writeSeries( "abc", 1, Arrays.asList( new DataPointValue( 100, 2.0f ) ) ) );
        Assert.assertEquals( "abc|100|1|2.35",
            DumpFormat.writeSeries( "abc", 1, Arrays.asList( new DataPointValue( 100, 2.351f ) ) ) );
        Assert.assertEquals(
            "abc|100|1|2.3,2.4",
            DumpFormat.writeSeries( "abc", 1,
                Arrays.asList( new DataPointValue( 100, 2.3f ), new DataPointValue( 101, 2.4f ) ) ) );
        Assert.assertEquals( "abc|100|1|2.3,2:2.4,3", DumpFormat.writeSeries( "abc", 1, Arrays.asList(
            new DataPointValue( 100, 2.3f ), new DataPointValue( 102, 2.4f ), new DataPointValue( 103, 3f ) ) ) );
    }

    @Test
    public void parseSeries()
    {
        Assert.assertEquals( Arrays.asList( new DataPoint( "abc", 2, 100 ) ), DumpFormat.parseSeries( "abc|100|1|2" ) );
        Assert.assertEquals( Arrays.asList( new DataPoint( "abc", 2.35, 100 ) ),
            DumpFormat.parseSeries( "abc|100|1|2.35" ) );
        Assert.assertEquals( Arrays.asList( new DataPoint( "abc", 2.3, 100 ), new DataPoint( "abc", 2.4, 101 ) ),
            DumpFormat.parseSeries( "abc|100|1|2.3,2.4" ) );
        Assert.assertEquals( Arrays.asList( new DataPoint( "abc", 2.3, 100 ), new DataPoint( "abc", 2.4, 102 ),
            new DataPoint( "abc", 3, 103 ) ), DumpFormat.parseSeries( "abc|100|1|2.3,2:2.4,3" ) );
    }
}
