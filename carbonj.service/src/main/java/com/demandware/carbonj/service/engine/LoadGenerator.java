/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.StopWatch;

public class LoadGenerator
{
    static String ip = "localhost";

    static int port = 56789;

    static final int PODS = 5;

    // static final int BLADESPERPOD = 100;

    static final int REALMSPERPOD = 10;

    static final int JVMPERREALM = 20;

    static final int AVG_METRICS_JVM = 100;

    static final int SUM_METRICS_JVM = 100;

    static final int ITERATIONS = 1000;

    static AtomicLong counter = new AtomicLong();

    static Random rand = new Random();

    static String msg( int pod, int r, int jvm, int m, String suffix )
    {
        counter.incrementAndGet();
        int rid = pod * 100 + r;
        return "pod" + pod + ".ecom." + rid + "." + rid + "_prd.blade-" + jvm + "." + rid
            + "_prd." + "ocapi.memcached.ProductWO.hit.m1_rate " + rand.nextLong() % 100000 + " "
            + ( System.currentTimeMillis() / 1000 );
    }

    static class Pod
        implements Runnable
    {
        final int podId;

        Pod( int id )
        {
            podId = id;
        }

        @Override
        public void run()
        {
            try (Socket clientSocket = new Socket( ip, port ))
            {
                PrintWriter pw = new PrintWriter( clientSocket.getOutputStream() );
                for ( int i = 0; i < ITERATIONS; i++ )
                {
                    for ( int r = 0; r < REALMSPERPOD; r++ )
                    {
                        for ( int j = 0; j < JVMPERREALM; j++ )
                        {
                            for ( int m = 0; m < AVG_METRICS_JVM; m++ )
                            {
                                pw.println( msg( podId, r, j, m, "mean" ) );
                            }
                            for ( int m = 0; m < SUM_METRICS_JVM; m++ )
                            {
                                pw.println( msg( podId, r, j, m, "sum" ) );
                            }
                            Thread.sleep( 1 );
                            pw.flush();
                        }
                        Thread.sleep( 5 );
                        pw.flush();
                    }
                    Thread.sleep( 60000 );
                    pw.flush();
                }
                pw.flush();
                pw.close();
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        }
    }

    public static void main( String[] s )
        throws Exception
    {
        StopWatch sw = new StopWatch();
        sw.start();
        Thread[] th = new Thread[PODS];
        for ( int i = 0; i < th.length; i++ )
        {
            th[i] = new Thread( new Pod( i ) );
            th[i].start();
        }
        for ( int i = 0; i < th.length; i++ )
        {
            th[i].join();
        }
        System.out.println( counter.get() + " messages sent in " + sw );
        System.out.println( "Send rate " + counter.get() * 1000 / sw.getTime() );
    }
}
