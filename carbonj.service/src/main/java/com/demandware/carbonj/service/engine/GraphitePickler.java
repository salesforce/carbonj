/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.db.model.Interval;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.google.common.base.Preconditions;

import net.razorvine.pickle.Opcodes;
import net.razorvine.pickle.PickleException;
import net.razorvine.pickle.Pickler;

/**
 * Pickles data to be sent in response to graphite requests.
 */
public class GraphitePickler extends Pickler implements ResponseStream
{
    private static final Logger log = LoggerFactory.getLogger(GraphitePickler.class);

    private TimeSource timeSource = TimeSource.defaultTimeSource();

    public GraphitePickler()
    {
        this( true );
    }

    public GraphitePickler( boolean useMemo )
    {
        super( useMemo );
    }

    private boolean closed = false;

    public GraphitePickler( OutputStream out ) throws IOException
    {
        this( true, out );
    }

    public GraphitePickler(boolean useMemo,  OutputStream out ) throws IOException
    {
        super( useMemo );
        this.out = out;
        recurse = 0;
        if ( useMemo )
            memo = new HashMap<>();
        this.out.write( Opcodes.PROTO );
        this.out.write( super.PROTOCOL );
    }

    /**
     * Start adding a series list while ignoring memoization (useful when no
     * list object actually exists)
     */
    @Override
    public void openSeriesList()
        throws IOException
    {
        // List level characters.
        // @see Pickler.put_collection(Collection<?> list)
        this.out.write( Opcodes.EMPTY_LIST );
        this.out.write( Opcodes.MARK );
    }

    /**
     * Start adding a series list with memoization (useful if you are creating
     * the series object and want to decrease its size)
     */
    private synchronized void openSeriesList( List<Series> seriesList ) throws IOException
    {
        // List level characters.
        // @see Pickler.put_collection(Collection<?> list)
        this.out.write( Opcodes.EMPTY_LIST );
        writeMemo( seriesList );
        this.out.write( Opcodes.MARK );
    }

    @Override
    public synchronized void writeSeries( Series s )
        throws IOException
    {
        // Originally
        // Map<String, Object> dict = new HashMap<>();
        //
        // dict.put( "name", s.name );
        // dict.put( "start", s.start );
        // dict.put( "end", s.end );
        // dict.put( "step", s.step );
        // dict.put( "values", s.values );
        // save( dict );

        // Map level characters.
        // @see Pickler.put_map(Map<?,?> o)
        out.write( Opcodes.EMPTY_DICT );
        writeMemo( s );
        out.write( Opcodes.MARK );

        // save(key1);
        // save(value1);
        // ...

        save("name");
        save( s.name );

        save( "start" );
        save( s.start );

        save( "end" );
        save( s.end );

        save( "step" );
        save( s.step );

        save( "values" );
        save( s.values );

        out.write( Opcodes.SETITEMS );
    }

    @Override
    public synchronized void closeSeriesList()
        throws IOException
    {
        this.out.write( Opcodes.APPENDS );
    }

    void pickleSeriesList( List<Series> seriesList ) throws IOException
    {
        Preconditions.checkNotNull( this.out );

        // Streaming version for writing a list of series.
        openSeriesList( seriesList );
        for ( Series s : seriesList )
        {
            writeSeries( s );
        }
        closeSeriesList();
        close();
    }

    @Override
    public synchronized void close() throws IOException
    {
        if( closed )
        {
            return;
        }

        closed = true;
        memo = null; // get rid of the memo table
        this.out.write( Opcodes.STOP );
        this.out.flush();
        if ( recurse != 0 ) // sanity check
            throw new PickleException( "recursive structure error, please report this problem" );
        this.out.close();
    }

    void pickleMetrics( List<Metric> nodes, OutputStream out)
                    throws IOException
    {
        this.out = out;
        recurse = 0;
        if ( useMemo )
            memo = new HashMap<>();
        this.out.write( Opcodes.PROTO );
        this.out.write( super.PROTOCOL );

        // Originally
        // save(o);

        // Streaming version for writing a list of series.

        // List level characters.
        // @see Pickler.put_collection(Collection<?> list)
        this.out.write( Opcodes.EMPTY_LIST );
        writeMemo( nodes );
        this.out.write( Opcodes.MARK );
        for ( Metric node : nodes )
        {
            try
            {
                Map<String, Object> entry = new HashMap<>();
                Interval interval = node.getMaxRetentionInterval( timeSource.getEpochSecond() );
                entry.put( "intervals", Collections.singletonList( tuple( interval.start, interval.end ) ) ); // TODO:
                                                                                                              // clean
                                                                                                              // it
                                                                                                              // up.
                entry.put( "isLeaf", node.isLeaf() );
                entry.put( "metric_path", node.name );

                save( entry );
            }
            catch ( Throwable t )
            {
                if ( node != null )
                {
                    log.error( String.format( "Failed to pickle data for metring [%s]", node.name ), t );
                }
                else
                {
                    log.error( "node is null" );
                }
            }
        }
        this.out.write( Opcodes.APPENDS );

        close();
    }

    private Object[] tuple( Object... values )
    {
        return values != null ? values : new Object[0];
    }

}
