/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.demandware.carbonj.service.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.demandware.carbonj.service.db.index.IndexUtils;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.google.common.io.Files;

/**
 *
 */
public class _Archives extends BaseTest
{
    private MetricIndex index;

    private DataPointStore archives;

    @Before
    public void setUp()
    {
        File dbDirFile = Files.createTempDir();
        index = IndexUtils.metricIndex( dbDirFile, false );
        index.open();
        archives = DataPointStoreUtils.createDataPointStore(metricRegistry, dbDirFile, false, index);
        archives.open();
    }

    @After
    public void tearDown()
    {
        index.close();
        archives.close();
    }

    private Metric findOrCreate( String name )
    {
        return IndexUtils.findOrCreate( index, name );
    }

    @Test
    public void insert()
    { // 1461351451
        Metric m = findOrCreate( "a.b.c.d" );
        int now = (int) ( System.currentTimeMillis() / 1000 );
        DataPoint p = new DataPoint( "a.b.c.d", 123.555, now );
        insert( m, p );

        List<DataPointValue> values = archives.getValues( RetentionPolicy._60s24h(), m.id, 0, Integer.MAX_VALUE );
        assertEquals( 1, values.size() );
        DataPointValue dpv = values.get( 0 );
        assertEquals( 123.555, dpv.val, 0.001 );

    }

    private void insert( Metric m, DataPoint p )
    {

        DataPoints points = new DataPoints( Collections.singletonList( p ) );
        points.assignMetric( 0, m, k -> k.getHighestPrecisionArchive().orElse( null ));
        archives.insertDataPoints( points );
    }

    @Test
    public void insertAndFindDataPoint()
        throws Exception
    {
        int now = (int) ( System.currentTimeMillis() / 1000 );
        DataPoint p = new DataPoint( "a.b.c.d", 123.555, now );
        Metric m1 = findOrCreate( p.name );
        insert( m1, p );
        Series s = archives.getSeries( m1, now, now, now );
        assertThat( s.name, equalTo( m1.name ) );
        assertThat( s.values.size(), equalTo( 1 ) );
        assertThat( s.values.get( 0 ), equalTo( p.val ) );
        System.out.println( "now: " + now );
        System.out.println( "start: " + s.start );
        System.out.println( "end: " + s.end );

        // Metric m2 = findOrCreate( "a.b.c.e" );
        // Metric m3 = findOrCreate( "a.b.c.d" );

        int rootId = 1;
        assertEquals( rootId + 1, m1.id );
        // assertEquals(2, m2.id);
        // assertEquals(1, m3.id);

        // List<Metric> metrics = findOrCreates( "*.*.*.*" );
        // assertEquals(2, metrics.size());
    }

    @Test
    @Ignore
    public void lotsOfPointsWithNewMetrics()
        throws Exception
    {
        ExecutorService es = Executors.newFixedThreadPool( 10 );
        long start = System.currentTimeMillis();
        for ( int i = 0; i < 10; i++ )
        {
            es.submit( new Task( i ) );
        }
        es.shutdown();
        es.awaitTermination( 30, TimeUnit.MINUTES );
        System.out.println( "total time: " + ( System.currentTimeMillis() - start ) );
    }

    private class Task
        implements Runnable
    {
        int i;

        public Task( int i )
        {
            this.i = i;
        }

        @Override
        public void run()
        {
            long start = System.currentTimeMillis();
            long iStart = System.currentTimeMillis();
            int counter = 0;
            for ( int j = 0; j < 10; j++ )
            {
                for ( int x = 0; x < 50; x++ )
                {
                    for ( int l = 0; l < 100; l++ )
                    {
                        counter++;
                        // insert( "iiiiiii" + i + ".jjjjjjj" + j + ".xxxxxxxxxxxxx" + x + ".llllllll" + l + ".count",
                        // 1234.888 );
                        if ( counter % 10000 == 0 )
                        {
                            System.out.println( i + ": Counter: " + counter + ", last insert took: "
                                + ( System.currentTimeMillis() - iStart ) + "ms" );
                            iStart = System.currentTimeMillis();
                        }
                        counter++;
                        // insert( "iiiiiii" + i + ".jjjjjjj" + j + ".xxxxxxxxxxxxx" + x + ".llllllll" + l + ".rate",
                        // 777.90);
                        if ( counter % 10000 == 0 )
                        {
                            System.out.println( i + ": Counter: " + counter + ", last insert took: "
                                + ( System.currentTimeMillis() - iStart ) + "ms" );
                            iStart = System.currentTimeMillis();
                        }
                    }
                }
            }

            long time = System.currentTimeMillis() - start;
            System.out.println( i + ": Took: " + time + "ms. Count: " + counter + ", Rate: " + counter / ( time / 1000 )
                + " per sec" );

            String key = "iiiiiii4.jjjjjjj50.xxxxxxxxxxxxx25.llllllll50.rate";
            long readStart = System.currentTimeMillis();
            Metric m = findOrCreate( key );
            System.out.println( i + " : " + ( System.currentTimeMillis() - readStart ) + "ms, id: " + m.id );
        }
    }

}
