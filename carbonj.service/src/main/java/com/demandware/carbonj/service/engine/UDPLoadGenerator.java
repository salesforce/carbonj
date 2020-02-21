/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.StopWatch;

/**
 * Clone of LoadGenerator.
 */
public class UDPLoadGenerator
{
    static String ip = "localhost";

    static int port = 56789;

    static final int PODS = 1;

    // static final int BLADESPERPOD = 100;

    static final int REALMSPERPOD = 1;

    static final int JVMPERREALM = 2;

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
            + "_prd.rpc.outgoing.protocols.baskets.get.time." + m + suffix + " " + 1 /*rand.nextLong() % 100000*/+ " "
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
            try (DatagramSocket clientSocket = new DatagramSocket())
            {
                for ( int i = 0; i < ITERATIONS; i++ )
                {
                    for ( int r = 0; r < REALMSPERPOD; r++ )
                    {
                        for ( int j = 0; j < JVMPERREALM; j++ )
                        {
                            for ( int m = 0; m < AVG_METRICS_JVM; m++ )
                            {
                                send( clientSocket, msg( podId, r, j, m, "mean" ) );
                            }
                            for ( int m = 0; m < SUM_METRICS_JVM; m++ )
                            {
                                send( clientSocket, msg( podId, r, j, m, "sum" ) );
                            }
                        }
                        Thread.sleep( 60 * 1000 );
                    }
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        }

        StringBuilder sb = new StringBuilder();

        private void send( DatagramSocket socket, String msg )
            throws Exception
        {
            sb.append( msg + "\n" );
            if ( sb.length() > 25 * 80 ) // somewhat close to what ECOM does.
            {
                byte[] data = sb.toString().getBytes();
                DatagramPacket packet = new DatagramPacket( data, 0, data.length, InetAddress.getByName( ip ), 56789 );
                socket.send( packet );
                sb = new StringBuilder();
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
