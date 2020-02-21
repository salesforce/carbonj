/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.admin;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;

import com.demandware.carbonj.service.db.model.DataPointValue;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.LineProtocolHandler;
import com.google.common.base.Throwables;

public class CarbonJClient
    implements AutoCloseable
{
    private static final Logger log = LoggerFactory.getLogger( CarbonJClient.class );

    private final InetAddress ip;

    private final int dataPort;

    private final int httpPort;

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    private Socket lineProtocolSock;

    public CarbonJClient( String ip, int httpPort, int dataPort )
    {
        this.dataPort = dataPort;
        this.httpPort = httpPort;
        log.info("CarbonjClient is using Dataport as " + dataPort + ", HttpPort as " + httpPort);

        try
        {
            this.ip = InetAddress.getByName( ip );
        }
        catch ( UnknownHostException e )
        {
            throw Throwables.propagate( e );
        }
    }

    @Override
    public void close()
    {
        if ( null != lineProtocolSock )
        {
            try
            {
                lineProtocolSock.close();
            }
            catch ( IOException e )
            {
            }
        }
        try
        {
            httpClient.close();
        }
        catch ( IOException e )
        {
            throw Throwables.propagate( e );
        }
    }

    private synchronized Socket openPlaintextStream()
    {
        try
        {
            if ( null == lineProtocolSock || lineProtocolSock.isClosed() )
            {
                lineProtocolSock = new Socket( ip, dataPort );
            }
            return lineProtocolSock;
        }
        catch ( IOException e )
        {
            throw Throwables.propagate( e );
        }
    }

    public synchronized void send( Writer os, DataPoint... dps )
    {
        try
        {
            for ( DataPoint dp : dps )
            {
                os.write( dp.toString() );
                os.write( "\n" );
            }
            os.flush();
        }
        catch ( IOException e )
        {
            throw Throwables.propagate( e );
        }
    }

    public void send( Collection<DataPoint> dp )
    {
        for ( DataPoint d : dp )
        {
            send( d );
        }
    }

    public void send( DataPoint dp )
    {
        Socket s = openPlaintextStream();
        OutputStreamWriter wrt;
        try
        {
            wrt = new OutputStreamWriter( s.getOutputStream() );
        }
        catch ( IOException e )
        {
            throw Throwables.propagate( e );
        }
        send( wrt, dp );
    }

    public void send( String name, double value, DateTime time )
    {
        send( new DataPoint( name, value, (int) ( time.getMillis() / 1000 ) ) );
    }

    private String cjAdminUrl( String cmd )
    {
        return "http://" + ip.getHostAddress() + ":" + httpPort + "/_dw/rest/carbonj/" + cmd;
    }

    public Collection<String> listMetrics( String pattern )
    {
        ArrayList<String> ret = new ArrayList<String>();
        // http://localhost:56787/_dw/rest/carbonj/listmetrics2/{pattern}
        doRestCall( "listmetrics2/" + pattern, e -> ret.add( e ) );
        return ret;
    }

    public String listPointsWithId( String dbName, String id )
    {
        ArrayList<String> ret = new ArrayList<String>();
        doRestCall( String.format( "/listpointswithid/%s/%s", dbName, id ), s -> ret.add( s ) );
        return ret.get( 0 );
    }

    private void doPost( String url, String body )
    {
        HttpPost post = new HttpPost( cjAdminUrl( url ) );
        HttpEntity entity;
        try
        {
            entity = new ByteArrayEntity( body.getBytes( "UTF-8" ) );
        }
        catch ( UnsupportedEncodingException e1 )
        {
            throw Throwables.propagate( e1 );
        }
        post.setEntity( entity );
        try (CloseableHttpResponse resp = httpClient.execute( post ))
        {
            if ( 200 != resp.getStatusLine().getStatusCode() )
            {
                throw new RuntimeException( "Failed response: " + resp.getStatusLine() );
            }
            HttpEntity e = resp.getEntity();
            try
            {
                System.out.println( IOUtils.readLines( e.getContent() ) );
            }
            finally
            {
                EntityUtils.consume( e );
            }
        }
        catch ( Exception e )
        {
            throw Throwables.propagate( e );
        }
    }

    private void doCall( HttpRequestBase req, Consumer<String> respHandler )
    {
        try (CloseableHttpResponse resp = httpClient.execute( req ))
        {
            if ( 200 != resp.getStatusLine().getStatusCode() )
            {
                throw new RuntimeException( "Failed response: " + resp.getStatusLine() );
            }
            HttpEntity e = resp.getEntity();
            LineIterator lines = IOUtils.lineIterator( e.getContent(), "UTF-8" );
            while ( lines.hasNext() )
            {
                String l = lines.nextLine();
                if ( StringUtils.isEmpty( l ) )
                {
                    continue;
                }
                respHandler.accept( l );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw Throwables.propagate( e );
        }
    }

    private void doRestCall( String url, Consumer<String> respHandler )
    {
        log.info("cjAdminUrl url is " + cjAdminUrl( url ));
        doCall( new HttpGet( cjAdminUrl( url ) ), respHandler );
    }

    public Collection<String> dumpNames( String filter )
    {
        return dumpNames( filter, null, null, null );
    }

    public Collection<String> dumpNames( String filter, Integer startId, String startName, Integer count )
    {
        ArrayList<String> ret = new ArrayList<String>();
        // http://localhost:56787/_dw/rest/carbonj/listmetrics2/{pattern}
        StringBuilder path = new StringBuilder( "dumpnames?" );
        if ( null != filter )
        {
            path.append( "filter=" + filter + "&" );
        }
        if ( null != startId )
        {
            path.append( "startId=" + startId + "&" );
        }
        if ( null != startName )
        {
            path.append( "startName=" + startName + "&" );
        }
        if ( null != count )
        {
            path.append( "count=" + count + "&" );
        }

        doRestCall( path.toString(), e -> ret.add( e ) );
        return ret;
    }

    public Collection<String> cleanSeries( String from, String filter, String exclude, Integer count, boolean dryRun )
    {
        ArrayList<String> ret = new ArrayList<String>();
        // http://localhost:56787/_dw/rest/carbonj/listmetrics2/{pattern}
        StringBuilder path = new StringBuilder( "cleanseries?dryRun=" + dryRun );
        if ( null != from )
        {
            path.append( "&from=" + from );
        }
        if ( null != count )
        {
            path.append( "&count=" + count );
        }
        if ( null != filter )
        {
            path.append( "&filter=" + filter );
        }
        if ( null != exclude )
        {
            path.append( "&exclude=" + exclude );
        }

        doCall( new HttpPost( cjAdminUrl( path.toString() ) ), e -> ret.add( StringUtils.substringBefore( e, ":" ) ) );
        return ret;
    }

    public Collection<DataPoint> dumpLines( String dbName, String startName, String filter, int from, int to )
    {
        ArrayList<DataPoint> ret = new ArrayList<>();
        StringBuilder sb = new StringBuilder( "dumplines/" + dbName );
        sb.append( "?start=" ).append( null != startName ? startName : "" );
        sb.append( "&filter=" ).append( null != filter ? filter : "" );
        sb.append( "&from=" ).append( from );
        sb.append( "&to=" ).append( to );
        doRestCall( sb.toString(), i -> {
            DataPoint dataPoint = LineProtocolHandler.parse(i);
            if (dataPoint != null) {
                ret.add(dataPoint);
            }
        });
        return ret;
    }

    public static class DumpResult
    {
        public final int cursor;

        public final List<DataPoint> data;

        public static boolean isDone( int cursor )
        {
            return Integer.MAX_VALUE == cursor;
        }

        DumpResult( List<DataPoint> result, int cursor )
        {
            this.data = result;
            this.cursor = cursor;
        }

        public boolean isDone()
        {
            return isDone( cursor );
        }
    }

    public DumpResult dumpSeries( String dbName, int cursor, int count, String filter, int from, int to )
    {
        return dumpSeries( dbName, cursor, count, filter, null, from, to );
    }

    public DumpResult dumpSeries( String dbName, int cursor, int count, String filter, String exclude, int from, int to )
    {
        ArrayList<DataPoint> data = new ArrayList<>();
        int next = dumpSeries( dbName, cursor, count, filter, exclude, from, to, d -> data.addAll( d ) );
        return new DumpResult( data, next );
    }

    public int dumpSeries( String dbName, int cursor, int count, String filter, String exclude, int from, int to,
                           Consumer<List<DataPoint>> dataHander )
    {
        StringBuilder sb = new StringBuilder( "dumpseries/" + dbName );
        sb.append( "?cursor=" ).append( cursor );
        sb.append( "&count=" ).append( count );
        sb.append( "&filter=" ).append( null != filter ? filter : "" );
        sb.append( "&exclude=" ).append( null != exclude ? exclude : "" );
        sb.append( "&from=" ).append( from );
        sb.append( "&to=" ).append( to );
        AtomicInteger ret = new AtomicInteger( Integer.MAX_VALUE );
        final Consumer<String> dumpConsumer = new Consumer<String>()
        {
            @Override
            public void accept( String i )
            {
                List<DataPoint> dp = DumpFormat.parseSeries( i );
                if ( i.startsWith( "ignore.dumpseries.cursor" ) )
                {
                    // special cursor value marking end of the stream
                    ret.set( (int) dp.get( 0 ).val );
                }
                else
                {
                    dataHander.accept( dp );
                }
            }
        };
        doRestCall( sb.toString(), dumpConsumer );
        return ret.get();
    }

    public void loadLines( String dbName, Collection<DataPoint> dp )
    {
        StringBuilder body = new StringBuilder();
        for ( DataPoint d : dp )
        {
            body.append( d.toString() ).append( "\n" );
        }
        doPost( "loadlines/" + dbName, body.toString() );
    }

    public void loadSeries( String dbName, int start, int step, Map<String, double[]> vals )
    {
        StringBuilder body = new StringBuilder();
        for ( Map.Entry<String, double[]> e : vals.entrySet() )
        {
            ArrayList<DataPointValue> v = new ArrayList<DataPointValue>();
            for ( int i = 0; i < e.getValue().length; i++ )
            {
                v.add( new DataPointValue( start + i * step, e.getValue()[i] ) );
            }
            body.append( DumpFormat.writeSeries( e.getKey(), step, v ) ).append( "\n" );
        }
        doPost( "loadseries/" + dbName, body.toString() );
    }

    public Map<Integer, Double> listPoints( String metric, String db )
    {
        TreeMap<Integer, Double> ret = new TreeMap<>();
        doRestCall( "listpoints2/" + db + "/" + metric, line -> {
            String[] parts = StringUtils.split( line, ' ' );
            ret.put( Integer.parseInt( parts[0] ), Double.parseDouble( parts[1] ) );
        } );
        return ret;
    }
}
