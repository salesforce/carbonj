/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class _NewIndex extends BaseIndexTest
{

    @Test
    public void canCreateMetricNameWithOneSegment() throws Exception
    {
        Metric m1 = findOrCreate( "a" );
        int rootId = 1;
        assertEquals(rootId + 1, m1.id);

        List<Metric> metrics = index.findMetrics( "*" );
        assertEquals(1, metrics.size());
    }

    @Test
    public void metricIndex() throws Exception
    {
        Metric m1 = findOrCreate( "a.b.c.d" );
        Metric m2 = findOrCreate( "a.b.c.e" );
        Metric m3 = findOrCreate( "a.b.c.d" );
        int rootId = 1;
        assertEquals(rootId + 1, m1.id);
        assertEquals(rootId + 2, m2.id);
        assertEquals(rootId + 1, m3.id);

        List<Metric> metrics = index.findMetrics( "*.*.*.*" );
        assertEquals(2, metrics.size());
    }

    private class Task implements Runnable
    {
        int i;
        public Task(int i)
        {
            this.i = i;
        }


        @Override
        public void run()
        {
            long start = System.currentTimeMillis();
            long iStart = System.currentTimeMillis();
            int counter = 0;
            for ( int j = 0; j < 20; j++ )
            {
                for ( int x = 0; x < 50; x++ )
                {
                    for( int y = 0; y < 1; y++ )
                    {
                        for( int z = 0; z < 1; z++ )
                        {
                            for ( int l = 0; l < 100; l++ )
                            {
                                counter++;
                                findOrCreate(
                                                "jjjjjjjjjj" + j + ".iiiiiii" + i + ".yyyy" + y + ".zzzz"+ z + ".xxxxxxxxxxxxx" + x + ".llllllll" + l + ".count" );
                                if ( counter % 10000 == 0 )
                                {
                                    System.out.println( i + ": Counter: " + counter + ", last insert took: " + ( System.currentTimeMillis() - iStart ) + "ms" );
                                    iStart = System.currentTimeMillis();
                                }
                                counter++;
                                findOrCreate(
                                                "jjjjjjjjjj" + j + ".iiiiiiiii" + i + ".yyyy" + y + ".zzzz" + z + ".xxxxxxxxxxxxx" + x
                                                                + ".llllllll" + l + ".rate" );
                                if ( counter % 10000 == 0 )
                                {
                                    System.out.println( i + ": Counter: " + counter + ", last insert took: " + ( System.currentTimeMillis() - iStart ) + "ms" );
                                    iStart = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                }
            }

            long time = System.currentTimeMillis() - start;
            System.out.println( i + ": Took: " + time + "ms. Count: " + counter + ", Rate: " + counter / (time / 1000) + " per sec");

            String key = "iiiiiii4.jjjjjjj50.xxxxxxxxxxxxx25.llllllll50.rate";
            long readStart = System.currentTimeMillis();
            Metric m = findOrCreate( key );
            System.out.println(i + " : " + (System.currentTimeMillis() - readStart) + "ms, id: " + m.id);
        }
    }

    @Test
    @Ignore
    public void lotsOfMetrics() throws Exception
    {
        ExecutorService es = Executors.newFixedThreadPool( 32 );
        long start = System.currentTimeMillis();
        for ( int i = 0; i < 8; i++ )
        {
            es.submit( new Task( i ) );
        }
        es.shutdown();
        es.awaitTermination( 30, TimeUnit.MINUTES ); //TODO: clean it up.
        System.out.println("total time: " + (System.currentTimeMillis() - start));
    }
}
