/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.demandware.carbonj.service.db.model.Interval;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.google.common.collect.Iterables;

import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class _GraphitePickler
{
    private final TimeSource timeSource = TimeSource.defaultTimeSource();

    private static final List<Series> series = new ArrayList<>();

    @BeforeAll
    public static void setUpBeforeClass()
    {
        for ( int i = 0; i < 20000; ++i )
        {
            List<Double> values = new ArrayList<>();
            for ( double d = 0.0; d < 1440.0; d++ )
            {
                values.add( d + i );
            }

            series.add( new Series( String.format( "test%d", i ), 0, 1440, 1, values ) );
        }
    }

    @Test
    @Disabled( "Generates string stream for comparison to pickling stream. Used for basic profiling." )
    public void testStringStream() throws IOException
    {
        ByteArrayOutputStream seriesString = new ByteArrayOutputStream();
        StringBuilder sb = new StringBuilder(); 
        for ( Series s : series )
        {
            seriesString.write( sb.append( s.name ).append( s.start ).append( s.end ).append( s.step )
                    .append( Iterables.toString( s.values ) ).toString().getBytes() );

            sb.setLength( 0 );
        }
    }

    @Test
    @Disabled( "Tests pickling series stream with memoization. Used for basic profiling." )
    public void testPicklingLotsOfSeriesWithMemo() throws IOException
    {
        ByteArrayOutputStream seriesPickleOut = new ByteArrayOutputStream();
        new GraphitePickler( seriesPickleOut ).pickleSeriesList( series );
    }

    @Test
    @Disabled( "Tests pickling series stream without memoization. Used for basic profiling." )
    public void testPicklingLotsOfSeriesWithoutMemo() throws IOException
    {
        ByteArrayOutputStream seriesPickleOut = new ByteArrayOutputStream();
        new GraphitePickler( false, seriesPickleOut ).pickleSeriesList( series );
    }

    @Test
    @Disabled( "Tests pickling series with memoization. Used for basic profiling." )
    public void testOriginalPicklingLotsOfSeriesWithMemo() throws PickleException, IOException
    {
        List<Map<String, Object>> convertedSeries = new ArrayList<>();
        for ( Series s : series )
        {
            Map<String, Object> dict = new HashMap<>();

            dict.put( "name", s.name );
            dict.put( "start", s.start );
            dict.put( "end", s.end );
            dict.put( "step", s.step );
            dict.put( "values", s.values );
            convertedSeries.add( dict );
        }

        ByteArrayOutputStream originalPickleOut = new ByteArrayOutputStream();
        new Pickler().dump( convertedSeries, originalPickleOut );
    }

    @Test
    @Disabled( "Tests pickling series without memoization. Used for basic profiling." )
    public void testOriginalPicklingLotsOfSeriesWithoutMemo() throws PickleException, IOException
    {
        List<Map<String, Object>> convertedSeries = new ArrayList<>();
        for ( Series s : series )
        {
            Map<String, Object> dict = new HashMap<>();

            dict.put( "name", s.name );
            dict.put( "start", s.start );
            dict.put( "end", s.end );
            dict.put( "step", s.step );
            dict.put( "values", s.values );
            convertedSeries.add( dict );
        }

        ByteArrayOutputStream originalPickleOut = new ByteArrayOutputStream();
        new Pickler( false ).dump( convertedSeries, originalPickleOut );
    }


    @Test
    @Disabled
    public void testPickleMetricsListOutputStream() throws PickleException, IOException
    {
        List<Metric> nodes = new ArrayList<>();
                
        nodes.add( new Metric( "Test 1", 1, null, new ArrayList<>(), Arrays.asList( "ch1", "ch2" ) ));
        List<RetentionPolicy> retentionPolicies = RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y" );
        nodes.add( new Metric( "Test 2", 1, null, retentionPolicies, null ) );

        List<Map<String, Object>> picklableNodes = new ArrayList<>();
        for ( Metric node : nodes )
        {
            try
            {
                Map<String, Object> entry = new HashMap<>();
                Interval interval = node.getMaxRetentionInterval( timeSource.getEpochSecond() );
                entry.put( "intervals", Collections.singletonList(interval.start) ); // TODO:
                                                                                                              // clean
                                                                                                              // it
                                                                                                              // up.
                entry.put( "isLeaf", node.isLeaf() );
                entry.put( "metric_path", node.name );
                picklableNodes.add( entry );
            }
            catch ( Throwable t )
            {
                if ( node != null )
                {
                    System.out.printf("Failed to pickle data for metring [%s]%n", node.name );
                }
                else
                {
                    System.out.println( "node is null" );
                }
                throw new RuntimeException(t);
            }
        }

        ByteArrayOutputStream originalPickleOut = new ByteArrayOutputStream();
        new Pickler().dump( picklableNodes, originalPickleOut );

        ByteArrayOutputStream metricsPickleOut = new ByteArrayOutputStream();
        new GraphitePickler().pickleMetrics( nodes, metricsPickleOut );

        assertArrayEquals( originalPickleOut.toByteArray(), metricsPickleOut.toByteArray() );
    }

    @Test
    @Disabled
    public void testPickleSeriesListOutputStream() throws PickleException, IOException
    {
        List<Series> series = new ArrayList<>();

        List<Double> values = new ArrayList<>();
        for ( double d = 0.0; d < 10.0; d++ )
        {
            values.add( d );
        }

        series.add( new Series( "test", 0, 100, 10, values ) );
        series.add( new Series( "test2", 0, 100, 10, values ) );

        List<Map<String, Object>> convertedSeries = new ArrayList<>();
        for ( Series s : series )
        {
            Map<String, Object> dict = new HashMap<>();

            dict.put( "values", s.values );
            dict.put( "name", s.name );
            dict.put( "start", s.start );
            dict.put( "end", s.end );
            dict.put( "step", s.step );

            convertedSeries.add( dict );
        }

        ByteArrayOutputStream originalPickleOut = new ByteArrayOutputStream();
        new Pickler().dump( convertedSeries, originalPickleOut );
        
        ByteArrayOutputStream seriesPickleOut = new ByteArrayOutputStream();
        new GraphitePickler( seriesPickleOut ).pickleSeriesList( series );
        
        assertEquals( originalPickleOut.toByteArray().length, seriesPickleOut.toByteArray().length );
    }
}
