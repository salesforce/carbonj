/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.admin.CarbonJClient.DumpResult;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class _CarbonJSvcTest
                extends AbstractCarbonJ_StoreTest
{
    protected int toBucketTime( DateTime dt )
    {
        // align to one minute resolution
        return (int) ( dt.getMillis() / 1000 / 60 * 60 );
    }

    @Test
    public void testDumpNames()
    {
        cjClient.send( "a.b.c", 1.0f, new DateTime() );
        cjClient.send( "a.b.d", 1.0f, new DateTime() );
        drain();

        assertEquals( Arrays.asList( "a.b.c", "a.b.d" ), cjClient.dumpNames( null ) );
        assertEquals( Arrays.asList( "a.b.c", "a.b.d" ), cjClient.dumpNames( "*" ) );
        assertEquals( Arrays.asList( "a.b.c", "a.b.d" ), cjClient.dumpNames( "a.*" ) );
        assertEquals(List.of("a.b.d"), cjClient.dumpNames( "*.d" ) );

        assertEquals(List.of("a.b.c"), cjClient.dumpNames( null, null, "a.b.c", 1 ) );
        assertEquals(List.of("a.b.d"), cjClient.dumpNames( null, null, "a.b.d", null ) );
    }

    @Test public void testCleanSeries_dryRun()
    {
        assertTrue( cjClient.cleanSeries( null, null, null, null, true ).isEmpty() );
        cjClient.send( "a.b.c", 1.0f, new DateTime().minusHours( 2 ) );
        cjClient.send( "a.b.d", 1.0f, new DateTime().minusHours( 1 ) );
        drain();

        assertEquals( Arrays.asList( "a.b.c", "a.b.d" ), cjClient.cleanSeries( "-10m", null, null, null, true ) );
        assertEquals(List.of("a.b.c"),
                        cjClient.cleanSeries( "-10m", "*.c", null, null, true ), "test filtering" );
        assertEquals(List.of("a.b.d"),
                        cjClient.cleanSeries( "-10m", null, "*.c", null, true ), "test exclusion" );
        assertEquals(List.of("a.b.c"), cjClient.cleanSeries( "-10m", null, null, 1, true ) );
        assertEquals(List.of("a.b.c"), cjClient.cleanSeries( "-90m", null, null, null, true ) );
        assertEquals(List.of(), cjClient.cleanSeries( "-3h", null, null, null, true ) );
        assertEquals(List.of(), cjClient.cleanSeries( "-30d", null, null, null, true ) );
    }

    @Test public void testCleanSeries()
    {
        assertTrue( cjClient.cleanSeries( null, null, null, null, true ).isEmpty() );
        cjClient.send( "a.b.1", 1.0f, new DateTime().minusHours( 2 ) );
        cjClient.send( "a.b.2", 1.0f, new DateTime().minusHours( 1 ) );
        cjClient.send( "a.b.3", 1.0f, new DateTime().minusHours( 2 ) );
        cjClient.send( "a.b.4", 1.0f, new DateTime().minusHours( 1 ) );
        drain();

        assertEquals( Arrays.asList( "a.b.1", "a.b.3" ), cjClient.cleanSeries( "-90m", null, null, null, false ) );
        assertEquals( Arrays.asList( "a.b.2", "a.b.4" ), cjClient.dumpNames( null ) );
    }

    @Test public void testDumpLines()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "a.2", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.2", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();

        assertEquals( dps, cjClient.dumpLines( "60s24h", null, null, 0, Integer.MAX_VALUE ) );

        // test filtering
        assertEquals( dps, cjClient.dumpLines( "60s24h", null, "*", 0, Integer.MAX_VALUE ) );
        assertEquals( dps, cjClient.dumpLines( "60s24h", null, "*.*", 0, Integer.MAX_VALUE ) );

        assertEquals( Arrays.asList( dps.get( 0 ), dps.get( 1 ) ),
                        cjClient.dumpLines( "60s24h", null, "a.*", 0, Integer.MAX_VALUE ) );
        assertEquals( Arrays.asList( dps.get( 0 ), dps.get( 2 ) ),
                        cjClient.dumpLines( "60s24h", null, "*.1", 0, Integer.MAX_VALUE ) );

        // test skipping
        assertEquals( dps, cjClient.dumpLines( "60s24h", "a.1", null, 0, Integer.MAX_VALUE ) );
        assertEquals( Collections.EMPTY_LIST,
                        cjClient.dumpLines( "60s24h", "nonExisting", null, 0, Integer.MAX_VALUE ) );
        assertEquals( Arrays.asList( dps.get( 2 ), dps.get( 3 ) ),
                        cjClient.dumpLines( "60s24h", "b.1", null, 0, Integer.MAX_VALUE ) );
    }

    @Test public void testDumpLines_filterTime()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f,
                                        DataPoint.align2Min( new DateTime().minusMinutes( 2 ) ) ),
                        new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();

        assertEquals( dps, cjClient.dumpLines( "60s24h", null, null, 0, Integer.MAX_VALUE ) );

        // test filtering
        assertEquals(Collections.singletonList(dps.get(0)),
                        cjClient.dumpLines( "60s24h", null, null, 0, dps.get( 0 ).ts + 1 ) );
        assertEquals(Collections.singletonList(dps.get(1)),
                        cjClient.dumpLines( "60s24h", null, null, dps.get( 1 ).ts - 1, Integer.MAX_VALUE ) );
    }

    @Test public void testDumpSeries()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "a.2", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.2", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();

        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, null, 0, 0 ).data );

        // test filtering
        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, "*", 0, 0 ).data );
        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, "*.*", 0, 0 ).data );

        assertEquals( Arrays.asList( dps.get( 0 ), dps.get( 1 ) ),
                        cjClient.dumpSeries( "60s24h", 0, 1000, "a.*", 0, 0 ).data );
        assertEquals( Arrays.asList( dps.get( 0 ), dps.get( 2 ) ),
                        cjClient.dumpSeries( "60s24h", 0, 1000, "*.1", 0, 0 ).data );
    }

    @Test public void testDumpSeries_excludeFilter()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "a.2", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.2", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();

        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, null, null, 0, 0 ).data );

        // test filtering
        assertEquals( Collections.EMPTY_LIST, cjClient.dumpSeries( "60s24h", 0, 1000, null, "*", 0, 0 ).data );
        assertEquals( Arrays.asList( dps.get( 1 ), dps.get( 3 ) ),
                        cjClient.dumpSeries( "60s24h", 0, 1000, null, "*.1", 0, 0 ).data );
        assertEquals(Collections.singletonList(dps.get(3)), cjClient.dumpSeries( "60s24h", 0, 1000, "b*", "*.1", 0, 0 ).data );
    }

    @Test public void testDumpSeries_cursor()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "a.2", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.1", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b.2", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();
        DumpResult dr = cjClient.dumpSeries( "60s24h", 0, 1, "*", 0, 0 );
        assertEquals(Collections.singletonList(dps.get(0)), dr.data );
        assertFalse( dr.isDone() );
        dr = cjClient.dumpSeries( "60s24h", dr.cursor, 2, "*", 0, 0 );
        assertEquals( Arrays.asList( dps.get( 1 ), dps.get( 2 ) ), dr.data );
        assertFalse( dr.isDone() );
        dr = cjClient.dumpSeries( "60s24h", dr.cursor, 2, "*", 0, 0 );
        assertEquals(Collections.singletonList(dps.get(3)), dr.data );
        assertTrue( dr.isDone() );
        dr = cjClient.dumpSeries( "60s24h", dr.cursor, 2, "*", 0, 0 );
        assertEquals( Collections.EMPTY_LIST, dr.data );
        assertTrue( dr.isDone() );
    }

    @Test public void testDumpSeries_filterTime()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a.1", 1.0f,
                                        DataPoint.align2Min( new DateTime().minusMinutes( 2 ) ) ),
                        new DataPoint( "a.1", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.send( dps );
        drain();

        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, null, 0, Integer.MAX_VALUE ).data );
        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, null, 0, 0 ).data );
        assertEquals( dps, cjClient.dumpSeries( "60s24h", 0, 1000, null,
                        dps.get( 0 ).ts - (int) ( System.currentTimeMillis() / 1000 ) - 1, 0 ).data );
        assertEquals( Collections.EMPTY_LIST, cjClient.dumpSeries( "60s24h", 0, 1000, null, -1, 0 ).data );

        // test filtering
        assertEquals(Collections.singletonList(dps.get(0)),
                        cjClient.dumpSeries( "60s24h", 0, 1000, null, dps.get( 0 ).ts - 1, dps.get( 1 ).ts - 1 ).data );
        assertEquals(Collections.singletonList(dps.get(1)),
                        cjClient.dumpSeries( "60s24h", 0, 1000, null, dps.get( 1 ).ts - 1, 0 ).data );
        assertEquals(Collections.singletonList(dps.get(0)), cjClient.dumpSeries( "60s24h", 0, 1000, null, 0,
                        dps.get( 1 ).ts - (int) ( System.currentTimeMillis() / 1000 ) - 1 ).data );
    }

    @Test public void testImportData()
    {
        List<DataPoint> dps = Arrays.asList( new DataPoint( "a", 1.0f, DataPoint.align2Min( new DateTime() ) ),
                        new DataPoint( "b", 1.0f, DataPoint.align2Min( new DateTime() ) ) );
        cjClient.loadLines( "60s24h", dps );
        drain();

        assertEquals( dps, cjClient.dumpLines( "60s24h", null, null, 0, Integer.MAX_VALUE ) );
    }

    @Test public void testListMetrics()
    {
        cjClient.send( "a", 1.0f, new DateTime() );
        cjClient.send( "b.c", 1.0f, new DateTime() );
        cjClient.send( "d.e.f", 1.0f, new DateTime() );
        cjClient.send( "d.e.g", 1.0f, new DateTime() );
        drain();

        assertEquals( Arrays.asList( "a", "b", "d" ), cjClient.listMetrics( "*" ) );
        assertEquals( Arrays.asList( "d.e.f", "d.e.g" ), cjClient.listMetrics( "*.*.*" ) );
        assertEquals(List.of("d.e.g"), cjClient.listMetrics( "*.*.g" ) );
        assertEquals(List.of("b.c"), cjClient.listMetrics( "b.*" ) );
    }

    @Test public void testData()
    {
        DateTime dt0 = new DateTime();
        DateTime dt1 = dt0.plusMinutes( 1 );

        cjClient.send( "testData", 1.0f, dt0 );
        cjClient.send( "testData", 2.0f, dt1 );
        drain();

        assertEquals( ImmutableMap.of( toBucketTime( dt0 ), 1.0d, toBucketTime( dt1 ), 2.0d ),
                        cjClient.listPoints( "testData", DB_60S ) );
    }

    @Test public void testData_100()
                    throws InterruptedException
    {
        final int SIZE = 100;
        DateTime dtNow = new DateTime();
        for ( int i = 0; i < SIZE; i++ )
        {
            cjClient.send( "testData", i, dtNow.minusMinutes( SIZE - i ) );
        }

        drain();
        Map<Integer, Double> result = cjClient.listPoints( "testData", DB_60S );
        assertEquals( SIZE, result.size() );
        double expected = 0f;
        for ( Double v : result.values() )
        {
            assertEquals( expected++, v, 0.1 );
        }
    }

    @Test
    public void testListPointWithId() throws InterruptedException
    {
        DateTime dt0 = new DateTime();
        cjClient.send( "testData", 1.0f, dt0 );
        cjClient.listPointsWithId( "60s24h", "0" );
        drain();
        System.out.println( cjClient.listPointsWithId( "60s24h", "0" ) );
    }
}
