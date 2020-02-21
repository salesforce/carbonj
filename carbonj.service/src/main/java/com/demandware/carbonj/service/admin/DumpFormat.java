/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.admin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.python.google.common.base.Preconditions;

import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.engine.DataPoint;

public class DumpFormat
{

    /**
     * Prints out time series in a compact format (<name>|<time0>|<step>|[index0:]val0,[index1:]val0,...,[indexN:]valN)
     * It relies on the fact that List<DataPointValue> is ordered by time and has a fixed increment - either 60s, 5m or
     * 30m. The time for associated datapoint is calculated as (time0 + step * index) The index is optional and if not
     * specified calculated as indexPrev+1. In case if there are missing datapoints it is enough to provide the explicit
     * index value to skip all the misses.
     */
    public static String writeSeries( String metricName, int stepSeconds, List<DataPointValue> vals )
    {
        Preconditions.checkArgument( !vals.isEmpty() );
        Preconditions.checkArgument( !metricName.contains( "|" ) );

        int startTs = vals.get( 0 ).ts;
        StringBuilder sb = new StringBuilder();
        sb.append( metricName ).append( "|" ).append( startTs ).append( "|" ).append( stepSeconds ).append( "|" );
        int idx = 0;
        for ( DataPointValue dp : vals )
        {
            if ( idx > 0 )
            {
                sb.append( "," );
            }
            boolean hole = false;
            for ( ;; )
            {
                int ts = startTs + stepSeconds * idx;
                idx++;
                if ( dp.ts <= ts )
                {
                    if ( hole )
                    {
                        sb.append( idx - 1 ).append( ":" );
                    }
                    sb.append( DataPoint.strValue( dp.val ) );
                    break;
                }
                hole = true;
            }
        }

        return sb.toString();
    }

    public static List<DataPoint> parseSeries( String s )
    {
        String[] parts = StringUtils.split( s, "|" );
        Preconditions.checkArgument( 4 == parts.length );
        String name = parts[0];
        int start = Integer.parseInt( parts[1] );
        int step = Integer.parseInt( parts[2] );
        ArrayList<DataPoint> ret = new ArrayList<>();
        int idx = 0;
        for ( String d : StringUtils.split( parts[3], "," ) )
        {
            String[] p = StringUtils.split( d, ":" );
            Preconditions.checkArgument( 1 <= p.length && p.length <= 2 );
            double val;
            if ( p.length == 1 )
            {
                val = Double.parseDouble( p[0] );
            }
            else
            {
                val = Double.parseDouble( p[1] );
                idx = Integer.parseInt( p[0] );
            }
            ret.add( new DataPoint( name, val, start + idx * step ) );
            idx++;
        }
        return ret;
    }

    public static void main( String[] v )
        throws FileNotFoundException
    {
        final File outFile = new File( "/tmp/cjtest/dump.100K.out" );
        int step = 5 * 60;
        int start = (int) ( new DateTime().minusDays( 7 ).getMillis() / 1000 );
        int points = 7 * 24 * 12;
        // quick tool to generate dump file for testing
        int NAME_LEV0 = 10;
        int NAME_LEV1 = 100;
        int NAME_LEV2 = 100;
        Random rand = new Random();
        FileUtils.deleteQuietly( outFile );
        try (PrintWriter out = new PrintWriter( outFile ))
        {
            for ( int i = 0; i < NAME_LEV0; i++ )
            {
                for ( int j = 0; j < NAME_LEV1; j++ )
                {
                    for ( int k = 0; k < NAME_LEV2; k++ )
                    {
                        String name = String.format( "level%s.level%s.level%s", i, j, k );
                        ArrayList<DataPointValue> dp = new ArrayList<DataPointValue>();
                        for ( int p = 0; p < points; p++ )
                        {
                            dp.add( new DataPointValue( start + step * p, rand.nextFloat() * 100 ) );
                        }
                        out.println( writeSeries( name, step, dp ) );
                    }
                }
            }
        }
    }
}
