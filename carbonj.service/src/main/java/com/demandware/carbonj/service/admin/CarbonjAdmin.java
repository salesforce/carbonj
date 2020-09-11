/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.admin;

import com.demandware.carbonj.service.admin.CarbonJClient.DumpResult;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.index.NameUtils;
import com.demandware.carbonj.service.db.model.*;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.DataPoints;
import com.demandware.carbonj.service.engine.InputQueue;
import com.demandware.carbonj.service.engine.LineProtocolHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Controller
@RequestMapping( value = "/_dw/rest/carbonj" )
public class CarbonjAdmin
{
    private static final Logger log = LoggerFactory.getLogger( CarbonjAdmin.class );

    private final Optional<TimeSeriesStore> tsStore;

    private final InputQueue inputQueue;

    private final NameUtils nameUtils;

    private Supplier<RuntimeException> notConfigured = ( ) -> new RuntimeException(
        "Time Series Store is not configured." );

    public CarbonjAdmin(InputQueue inputQueue, NameUtils nameUtils, Optional<TimeSeriesStore> tsStore )
    {
        this.inputQueue = Preconditions.checkNotNull(inputQueue);
        this.nameUtils = Preconditions.checkNotNull( nameUtils );
        this.tsStore = tsStore;
    }

    private TimeSeriesStore tsStore()
    {
        return this.tsStore.orElseThrow( notConfigured );
    }

    @ResponseStatus( value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "" )
    @ExceptionHandler( Throwable.class )
    public @ResponseBody Map<String, Object> handleExceptions( Throwable e )
    {
        log.error("Error: ", e );

        Map<String, Object> ret = new LinkedHashMap<>();

        ret.put( "status", "ERROR" );
        ret.put( "message", e.getMessage() );

        return ret;
    }

    /**
     * @deprecated use /listMetrics2 instead
     */
    @Deprecated
    @RequestMapping( value = "/listmetrics/{pattern:.+}", method = RequestMethod.GET )
    public @ResponseBody List<String> listMetrics( @PathVariable final String pattern )
        throws IOException
    {
        return tsStore().findMetrics( pattern ).stream().map( m -> m.name ).collect( Collectors.toList() );
    }

    /**
     * Lists metric names based on provided pattern.
     */
    // ":.+" is added to prevent spring from dropping the last segment of metric name
    @RequestMapping( value = "/listmetrics2/{pattern:.+}", method = RequestMethod.GET )
    public void listMetrics2( @PathVariable final String pattern, Writer response )
        throws IOException
    {
        tsStore().findMetrics( pattern ).stream().forEach( m -> {
            try
            {
                response.write( m.name + "\n" );
            }
            catch ( Exception e )
            {
                throw Throwables.propagate( e );
            }
        } );
    }

    @RequestMapping( value = "/dumpnames", method = RequestMethod.GET )
    public void dumpNames( @RequestParam( value = "startId", required = false, defaultValue = "0" ) long startId,
                           @RequestParam( value = "startName", required = false ) String startName,
                           @RequestParam( value = "count", required = false ) Integer count,
                           @RequestParam( value = "filter", required = false ) String wildcard, Writer response )
        throws IOException
    {
        Predicate<Metric> filter = wildcardFilter( wildcard );
        AtomicInteger res = new AtomicInteger();
        if ( !StringUtils.isEmpty( startName ) )
        {
            Metric m = tsStore().getMetric( startName, false );
            if ( null != m )
            {
                startId = m.id;
            }
        }
        try
        {
            tsStore().scanMetrics( startId, Long.MAX_VALUE, m -> {
                if ( !filter.test( m ) )
                {
                    return;
                }
                try
                {
                    response.write( m.name + "\n" );
                    int prodused = res.incrementAndGet();
                    if ( null != count && count <= prodused )
                    {
                        throw new StopException();
                    }
                }
                catch ( Exception e )
                {
                    throw Throwables.propagate( e );
                }
            } );
        }
        catch ( StopException e )
        {

        }
    }

    private boolean loadLock = false;

    private volatile boolean abortLoad = false;

    class LoadLock
        implements AutoCloseable
    {
        LoadLock()
        {
            synchronized ( CarbonjAdmin.this )
            {
                // simple exclusive locking to prevent multiple operations at the same time
                if ( loadLock )
                {
                    throw new RuntimeException( "Have operation in progress" );
                }
                loadLock = true;
                abortLoad = false;
            }

        }

        @Override
        public void close()
        {
            synchronized ( CarbonjAdmin.this )
            {
                loadLock = false;
                abortLoad = false;
            }
        }

    }

    static class AbortException
        extends RuntimeException
    {
        private static final long serialVersionUID = 1L;
    }

    @RequestMapping( value = "/loadseriesfrom/{dbName}", method = RequestMethod.POST )
    public void loadSeriesFrom( @PathVariable final String dbName,
                                @RequestParam( value = "srcIp", required = true ) String srcIp,
                                @RequestParam( value = "srcPort", required = true ) int srcPort,
                                @RequestParam( value = "from", required = false, defaultValue = "0" ) int from,
                                @RequestParam( value = "to", required = false, defaultValue = "0" ) int to,
                                @RequestParam( value = "cursor", required = false, defaultValue = "0" ) int cursor,
                                @RequestParam( value = "batchSize", required = false, defaultValue = "10000" ) int batchSize,
                                @RequestParam( value = "filter", required = false ) String wildcard,
                                @RequestParam( value = "exlcude", required = false ) String exclude,
                                @RequestParam( value = "bufferSize", required = false, defaultValue = "10" ) int readBufferSize,
                                @RequestParam( value = "maxRate", required = false, defaultValue = "100000" ) int maxRateSec,
                                @RequestParam( value = "maxQueue", required = false, defaultValue = "0" ) int maxQueueLevel,
                                @RequestParam( value = "delayBeforeRetrySec", required = false, defaultValue = "5" ) int delayBeforeRetrySec,
                                final @RequestParam( value = "retry", required = false, defaultValue = "2" ) int retry,
                                Writer response )
        throws IOException
    {
        try (LoadLock ll = new LoadLock())
        {
            log.info( String
                .format(
                    "Strting loadseriesfrom(dbname=%s, srcIp=%s, srcPort=%s, from=%s, to=%s, cursor=%s, batchSize=%s, filter=%s",
                    dbName, srcIp, srcPort, from, to, cursor, batchSize, wildcard ) );
            // for iterations need to have a stable time interval
            int fromRange = fromParam2Range( from );
            int toRange = untilParam2Range( to );
            log.info( String.format( "loadseriesfrom for stable interval %s - %s", fromRange, toRange ) );

            final int maxInputQueueLevel = 0 >= maxQueueLevel ? inputQueue.queueCapacity() / 2 : maxQueueLevel;

            ThreadPoolExecutor writerQueue =
                new ThreadPoolExecutor( 1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(
                    readBufferSize ), new RejectedExecutionHandler()
                {
                    @Override
                    public void rejectedExecution( Runnable r, ThreadPoolExecutor executor )
                    {
                        try
                        {
                            executor.getQueue().put( r );
                        }
                        catch ( InterruptedException e )
                        {
                            throw Throwables.propagate( e );
                        }
                    }
                } );
            try (CarbonJClient cjClient = new CarbonJClient( srcIp, srcPort, 0 ))
            {
                AtomicInteger metricsCount = new AtomicInteger();
                AtomicLong pointsCount = new AtomicLong();
                final TimeSeriesStore ts = tsStore();
                AtomicBoolean brokenResponseStream = new AtomicBoolean();
                int retryAttemptsLeft = retry;
                AtomicLong quotaSec = new AtomicLong( maxRateSec );
                AtomicLong quotaReset = new AtomicLong();

                while ( !DumpResult.isDone( cursor ) )
                {
                    final int currCurs = cursor;
                    log.info( String.format( "Requesting data (%s:%s cursor=%s)", srcIp, srcPort, cursor ) );
                    try
                    {
                        cursor =
                            cjClient.dumpSeries(
                                dbName,
                                cursor,
                                batchSize,
                                wildcard,
                                exclude,
                                fromRange,
                                toRange,
                                dps -> {
                                    if ( abortLoad )
                                    {
                                        throw new AbortException();
                                    }

                                    writerQueue.submit( ( ) -> {
                                        // throtling loop to limit import rate. Take into account current input
                                        // queue size
                                        while ( quotaSec.get() < 0 )
                                        {
                                            // respect abort request
                                            if ( abortLoad )
                                            {
                                                throw new AbortException();
                                            }
                                            try
                                            {
                                                long now = System.currentTimeMillis();
                                                if ( now >= quotaReset.get() )
                                                {
                                                    if ( inputQueue.queuedPointsCount() > maxInputQueueLevel )
                                                    {
                                                        // slow down to get queue processed to avoid overload
                                                        Thread.sleep( 1000 );
                                                        continue;
                                                    }

                                                    // we get to the next second: reset quota and move forward
                                                    quotaSec.set( maxRateSec );
                                                    quotaReset.set( now + 1000 ); // one sec ahead
                                                }
                                                else
                                                {
                                                    Thread.sleep( quotaReset.get() - now );
                                                }
                                            }
                                            catch ( Exception e )
                                            {
                                                throw Throwables.propagate( e );
                                            }
                                        }
                                        ts.importPoints( dbName, new DataPoints( dps ) );
                                        quotaSec.addAndGet( -1 * dps.size() );
                                        // tracking progress
                                        pointsCount.addAndGet( dps.size() );
                                        metricsCount.incrementAndGet();
                                        if ( !brokenResponseStream.get() )
                                        {
                                            try
                                            {
                                                response.write( dps.get( 0 ).name + ":" + dps.get( 0 ).metricId + " "
                                                    + dps.size() + " totalPoints:" + pointsCount.get() + " cursor:"
                                                    + currCurs + " totalMetrics:" + metricsCount.get() + "\n" );
                                                response.flush();
                                            }
                                            catch ( IOException e )
                                            {
                                                log.error( "Can not write", e );
                                                brokenResponseStream.set( true );
                                            }
                                        }
                                    } );
                                } );
                        log.info( String.format( "Stored %s metrics; %s datapoint; next cursor %s", metricsCount.get(),
                            pointsCount.get(), cursor ) );
                    }
                    catch ( AbortException a )
                    {
                        // let abort exception flow
                        throw a;
                    }
                    catch ( Exception e )
                    {
                        if ( retryAttemptsLeft-- > 0 )
                        {
                            log.warn( "Failure loading data. Operation will be retried. Retries left "
                                + retryAttemptsLeft, e );
                            Thread.sleep( delayBeforeRetrySec * 1000 );
                            continue;
                        }
                        else
                        {
                            throw Throwables.propagate( e );
                        }
                    }
                    // if completed successfully reset retry attempts
                    retryAttemptsLeft = retry;
                }
            }
            catch ( AbortException e )
            {
                log.info( "Operation has been aborted by user" );
            }
            catch ( Exception e )
            {
                log.error( "Load failed unexpectedly ", e );
                throw Throwables.propagate( e );
            }
            finally
            {
                writerQueue.shutdown();
                try
                {
                    if ( !writerQueue.awaitTermination( 5, TimeUnit.SECONDS ) )
                    {
                        log.warn( "Thread pool has not finished within 5 seconds" );
                    }
                }
                catch ( InterruptedException e )
                {
                    log.warn("Warning: ", e );
                     Throwables.propagate( e );
                }
            }
        }
    }

    @RequestMapping( value = "/abortload", method = RequestMethod.POST )
    public @ResponseBody String abortLoad()
    {
        boolean isRunning = loadLock;
        abortLoad = true;
        return isRunning ? "active load activity will be aborted" : "load is not running";
    }

    @RequestMapping( value = "/loadseries/{dbName}", method = RequestMethod.POST )
    public void loadSeries( @PathVariable final String dbName, HttpServletRequest req, Writer response )
        throws IOException
    {
        try (LoadLock l = new LoadLock())
        {
            final TimeSeriesStore ts = tsStore();
            ServletInputStream sis = req.getInputStream();
            LineIterator li = IOUtils.lineIterator( sis, Charset.forName( "UTF-8" ) );
            long count = 0;
            int seriesCount = 0;
            String lastLoadedMetric = null;
            boolean brokenResponseStream = false;
            try
            {
                while ( li.hasNext() && !abortLoad )
                {
                    String line = li.nextLine();
                    if ( StringUtils.isEmpty( line ) || line.startsWith( "ignore." ) ) // ignore - to facilitate cursor
                                                                                       // protocol
                    {
                        continue;
                    }
                    List<DataPoint> dps = DumpFormat.parseSeries( line );
                    ts.importPoints( dbName, new DataPoints( dps ) );

                    // tracking progress
                    seriesCount++;
                    count += dps.size();
                    lastLoadedMetric = dps.get( 0 ).name;
                    log.debug( String.format( "loadSeries: progress %s points=%s totalPoints=%s totalSeries=%s",
                        dps.get( 0 ).name, dps.size(), count, seriesCount ) );
                    if ( !brokenResponseStream )
                    {
                        try
                        {
                            response.write( dps.get( 0 ).name + " " + dps.size() + " " + count + " " + seriesCount
                                + "\n" );
                            response.flush();
                        }
                        catch ( IOException e )
                        {
                            log.error( "Can not write", e );
                            brokenResponseStream = true;
                        }
                    }
                }
            }
            finally
            {
                log.info( String.format( "loadSeries: last %s totalPoints=%s totalSeries=%s", lastLoadedMetric, count,
                    seriesCount ) );
            }
        }
    }

    @RequestMapping( value = "/loadlines/{dbName}", method = RequestMethod.POST )
    public void loadLines( @PathVariable final String dbName,
    /*@RequestParam( value = "start", required = false ) String startName,
    @RequestParam( value = "batch", required = false, defaultValue = "10000" ) int batchSize,*/
    HttpServletRequest req, Writer response )
        throws IOException
    {
        int batchSize = 10000;

        final TimeSeriesStore ts = tsStore();
        ServletInputStream sis = req.getInputStream();
        LineIterator li = IOUtils.lineIterator( sis, Charset.forName( "UTF-8" ) );
        final ArrayList<DataPoint> batch = new ArrayList<>( batchSize );
        int count = 0;
        while ( li.hasNext() )
        {
            String line = li.nextLine();
            if ( StringUtils.isEmpty( line ) )
            {
                continue;
            }
            DataPoint dataPoint = LineProtocolHandler.parse(line);
            if (dataPoint != null) {
                batch.add(dataPoint);
                if ( batch.size() >= batchSize )
                {
                    count = flushImportBatch( ts, dbName, count, batch, response );
                }
            }
        }
        flushImportBatch( ts, dbName, count, batch, response );
    }

    private static int flushImportBatch( TimeSeriesStore ts, String dbName, int count, List<DataPoint> batch, Writer out )
    {
        if ( batch.isEmpty() )
        {
            return count;
        }
        try
        {
            ts.importPoints( dbName, new DataPoints( batch ) );
            count += batch.size();
            try
            {
                out.write( count + " " + batch.get( batch.size() - 1 ).name + "\n" );
                out.flush();
            }
            catch ( IOException e )
            {
                throw Throwables.propagate( e );
            }
        }
        finally
        {
            batch.clear();
        }

        return count;
    }

    static class StopException
        extends RuntimeException
    {
        private static final long serialVersionUID = 1L;
    }

    static boolean hasDataSince( TimeSeriesStore ts, String metric, int from )
    {
        for ( String dbName : Arrays.asList( "30m2y", "5m7d", "60s24h" ) )
        {
            if ( null != ts.getFirst( dbName, metric, from, Integer.MAX_VALUE ) )
            {
                return true;
            }
        }
        return false;
    }

    @RequestMapping( value = "/cleanseries", method = RequestMethod.POST )
    public void cleanSeries( @RequestParam( value = "from", required = false, defaultValue = "-30d" ) String fromStr,
                             @RequestParam( value = "dryRun", required = false, defaultValue = "true" ) boolean dryRun,
                             @RequestParam( value = "count", required = false, defaultValue = "100000" ) int count,
                             @RequestParam( value = "filter", required = false ) String wildcard,
                             @RequestParam( value = "exclude", required = false ) String exclude, Writer response )
        throws IOException
    {
        final int from = str2Time( fromStr );
        final TimeSeriesStore ts = tsStore();
        AtomicInteger written = new AtomicInteger();
        Predicate<Metric> filter = wildcardFilter( wildcard );
        Predicate<Metric> excludeFilter = excludeFilter( exclude );

        try
        {
            ts.scanMetrics( 0, Integer.MAX_VALUE, m -> {
                if ( written.get() >= count )
                {
                    // produced big enough result - interrupt execution through exception (signal "donness")
                    // make sure that we have produced at least one metric to ensure that response is not empty.
                throw new StopException();
            }
            // test metric name based on the provided filter
                if ( !filter.test( m ) || excludeFilter.test( m ) )
                {
                    return;
                }
                try
                {
                    Metric del = null;
                    if ( !nameUtils.isValid( m.name, false ) )
                    {
                        response.write( "!" + m.name + ":" + written.get() + "\n" ); // invalid metric name
                        del = m;
                    }
                    else if ( !hasDataSince( ts, m.name, from ) )
                    {
                        response.write( m.name + ":" + written.get() + "\n" ); // no data
                        del = m;
                    }
                    if ( null != del )
                    {
                        if ( !dryRun )
                        {
                            ts.deleteMetric( m.name, false, false );
                        }
                        written.incrementAndGet();
                    }
                }
                catch ( Exception e )
                {
                    throw Throwables.propagate( e );
                }
            } );
        }
        catch ( StopException e )
        {
            // done
        }
    }

    // TODO SB: pagination does not take into account
    @RequestMapping( value = "/dumpseries/{dbName}", method = RequestMethod.GET )
    public void dumpSeries( @PathVariable final String dbName,
                            @RequestParam( value = "from", required = false, defaultValue = "0" ) int from,
                            @RequestParam( value = "to", required = false, defaultValue = "0" ) int to,
                            @RequestParam( value = "cursor", required = false, defaultValue = "0" ) int cursor,
                            @RequestParam( value = "count", required = false, defaultValue = "100000" ) int count,
                            @RequestParam( value = "filter", required = false ) String wildcard,
                            @RequestParam( value = "exclude", required = false ) String exclude, Writer response )
        throws IOException
    {
        final int fromRange = fromParam2Range( from );
        final int toRange = untilParam2Range( to );
        final TimeSeriesStore ts = tsStore();
        RetentionPolicy policy = RetentionPolicy.getInstanceForDbName( dbName );
        AtomicInteger written = new AtomicInteger();
        Predicate<Metric> filter = wildcardFilter( wildcard );
        Predicate<Metric> excludeFilter = excludeFilter( exclude );

        try
        {
            ts.scanMetrics( cursor,
                Integer.MAX_VALUE,
                m -> {
                    try
                    {
                        if ( written.get() >= count )
                        {
                            // add artificial metric to transfer cursor state.
                response.write( DumpFormat.writeSeries( "ignore.dumpseries.cursor", 1,
                    Arrays.asList( new DataPointValue( 0, m.id ) ) ) );
                response.write( "\n" );
                // produced big enough result - interrupt execution through exception (signal "donness")
                // make sure that we have produced at least one metric to ensure that response is not empty.
                throw new StopException();
            }
            if ( !nameUtils.isValid( m.name, false ) )
            {
                // skip metrics with invalid names
                return;
            }
            // test metric name based on the provided filter
            if ( !filter.test( m ) || excludeFilter.test( m ) )
            {
                return;
            }
            List<DataPointValue> vals = ts.getValues( dbName, m.name, fromRange, toRange );
            if ( vals.isEmpty() )
            {
                // skip
                return;
            }
            response.write( DumpFormat.writeSeries( m.name, policy.precision, vals ) );
            response.write( "\n" );
            written.incrementAndGet();
        }
        catch ( Exception e )
        {
            throw Throwables.propagate( e );
        }
    }       );

        }
        catch ( StopException e )
        {
            // done
        }
    }

    static int untilParam2Range( int v )
    {
        if ( 0 < v )
        {
            return v;
        }
        if ( 0 == v )
        {
            return (int) ( System.currentTimeMillis() / 1000 );
        }
        return (int) ( System.currentTimeMillis() / 1000 ) + v;
    }

    static int str2Time( String s )
    {
        char t = s.charAt( s.length() - 1 );
        switch ( t )
        {
            case 's': // sec
            case 'm': // min
            case 'h': // hour
            case 'd': // day
                s = s.substring( 0, s.length() - 1 );
                break;
        }
        int sec = Integer.parseInt( s );
        switch ( t )
        {
            case 'm':
                sec *= 60;
                break;
            case 'h':
                sec *= 60 * 60;
                break;
            case 'd':
                sec *= 60 * 60 * 24;
                break;
        }
        if ( 0 <= sec )
        {
            return sec;
        }
        return (int) ( System.currentTimeMillis() / 1000 ) + sec;
    }

    static int fromParam2Range( int v )
    {
        if ( 0 <= v )
        {
            return v;
        }
        return (int) ( System.currentTimeMillis() / 1000 ) + v;
    }

    static Predicate<Metric> wildcardFilter( String pattern )
    {
        return StringUtils.isEmpty( pattern ) ? a -> a.isLeaf() : a -> a.isLeaf()
            && FilenameUtils.wildcardMatch( a.name, pattern, IOCase.SENSITIVE );
    }

    static Predicate<Metric> excludeFilter( String pattern )
    {
        if ( StringUtils.isEmpty( pattern ) )
        {
            return a -> false;
        }
        return a -> FilenameUtils.wildcardMatch( a.name, pattern, IOCase.SENSITIVE );
    }

    @RequestMapping( value = "/dumplines/{dbName}", method = RequestMethod.GET )
    public void dumpLines( @PathVariable final String dbName,
                           @RequestParam( value = "from", required = false, defaultValue = "0" ) int from,
                           @RequestParam( value = "to", required = false, defaultValue = "0" ) int to,
                           @RequestParam( value = "start", required = false ) String startName,
                           @RequestParam( value = "filter", required = false ) String wildcard, Writer response )
        throws IOException
    {
        final TimeSeriesStore ts = tsStore();
        AtomicBoolean skip = new AtomicBoolean( !StringUtils.isEmpty( startName ) );
        Predicate<Metric> filter = wildcardFilter( wildcard );

        final int fromRange = fromParam2Range( from );
        final int toRange = untilParam2Range( to );

        ts.scanMetrics( m -> {
            if ( skip.get() )
            {
                if ( !m.name.equals( startName ) )
                {
                    return;
                }
                skip.set( false );
            }
            // test metric name based on the provided filter
            if ( !filter.test( m ) )
            {
                return;
            }
            try
            {
                for ( DataPointValue dv : ts.getValues( dbName, m.name, fromRange, toRange ) )
                {
                    response.write( m.name + " " + DataPoint.strValue( dv.val ) + " " + dv.ts + "\n" );
                }
            }
            catch ( Exception e )
            {
                throw Throwables.propagate( e );
            }
        } );
    }

    /**
     * Lists metric data points present in specified database.
     */
    @RequestMapping( value = "/listpoints2/{dbName}/{metricName:.+}", method = RequestMethod.GET )
    public void listPoints2( @PathVariable final String dbName, @PathVariable final String metricName, Writer response )
        throws IOException
    {
        DataPointExportResults res = tsStore().exportPoints( dbName, metricName );
        for ( DataPointValue v : res.values )
        {
            response.write( v.ts + " " + DataPoint.strValue( v.val ) + "\n" );
        }
    }

    /**
     * @deprecated use /listpoints2 instead
     */
    @Deprecated
    @RequestMapping( value = "/listpoints/{dbName}/{metricName:.+}", method = RequestMethod.GET )
    public @ResponseBody List<String> listPoints( @PathVariable final String dbName,
                                                  @PathVariable final String metricName )
        throws IOException
    {
        return Lists.transform( tsStore().exportPoints( dbName, metricName ).values, v -> v.toString() );
    }

    /**
     * Lists metric data points present in specified database.
     *
     * @deprecated use /listpoints2 instead
     */
    @Deprecated
    @RequestMapping( value = "/listpointswithid/{dbName}/{metricId:.+}", method = RequestMethod.GET )
    public @ResponseBody List<String> listPoints( @PathVariable final String dbName, @PathVariable final int metricId )
        throws IOException
    {
        DataPointExportResults res = tsStore().exportPoints( dbName, metricId );
        return Lists.transform( res.values, v -> v.toString() );
    }

    /**
     * Returns metric definition without the time series data.
     */
    @RequestMapping( value = "/metrics/{name:.+}", method = RequestMethod.GET )
    public @ResponseBody Metric findMetric( @PathVariable final String name )
        throws IOException
    {
        Metric m = tsStore().getMetric( name );
        if ( m != null )
        {
            return m;
        }
        else
        {
            throw new RuntimeException( String.format( "Metric [%s] not found.", name ) );
        }
    }

    /**
     * Returns metric definition without the time series data.
     */
    @RequestMapping( value = "/metricsearch", method = RequestMethod.GET )
    public @ResponseBody List<Metric> findMetric( @RequestParam( name = "metricId", required = false, defaultValue = "0" ) int metricId )
        throws IOException
    {
        if ( metricId <= 0 )
        {
            throw new RuntimeException( String.format(
                "request parameter 'metricId' should be a positive int, Actual value [%s]", metricId ) );
        }

        Metric m = tsStore().getMetric( metricId );
        if ( m != null )
        {
            return Arrays.asList( m );
        }
        else
        {
            throw new RuntimeException( String.format( "Metric with id [%s] not found.", metricId ) );
        }
    }

    @RequestMapping( value = "/getIdStoreName/{name:.+}", method = RequestMethod.GET )
    public @ResponseBody String getIdStoreName( @PathVariable final String name )
            throws IOException
    {
        Metric m = tsStore().getMetric( name );
        if ( m != null )
        {
            return tsStore().getMetricName(m.id);
        }
        else
        {
            throw new RuntimeException( String.format( "Metric [%s] not found.", name ) );
        }
    }

    /**
     * Removes metric name and associated data points from all databases.
     */
    @RequestMapping( value = "/metrics/{name:.+}", method = RequestMethod.DELETE )
    public @ResponseBody List<String> deleteMetric( @PathVariable final String name,
                                                    @RequestParam( name = "testRun", required = true ) boolean testRun,
                                                    @RequestParam( name = "force", required = false, defaultValue = "false" ) boolean force )
        throws IOException
    {
        return tsStore().deleteMetric( name, force, testRun ).stream().map( m -> m.name ).collect( Collectors.toList() );
    }


    @RequestMapping( value = "/metrics/deleteAPI/{name:.+}", method = RequestMethod.DELETE )
    public @ResponseBody
    DeleteAPIResult deleteAPI( @PathVariable final String name,
                               @RequestParam( name = "delete", required = true ) boolean delete,
                               @RequestParam( name = "exclude", required = false ) List<String> exclude )
            throws IOException    {        Stopwatch timer = Stopwatch.createStarted();
        Set<String> excludeSet = Collections.EMPTY_SET;
        if( exclude != null &&  !exclude.isEmpty())
        {
            excludeSet = new HashSet<>(exclude);
        }

        DeleteAPIResult result;
        try
        {
            result = tsStore().deleteAPI(name, !delete, excludeSet);
            result.setResponseTtime( timer.stop().elapsed().toMillis() );
            result.setSuccess(true);
        }
        catch (Throwable t)
        {
            result = new DeleteAPIResult();
            result.setError( t.getMessage() );
        }
        return result;
    }


    /**
     * Finds all metrics recursively starting from specified name.
     */
    @RequestMapping( value = "/findallmetrics/{name:.+}", method = RequestMethod.GET )
    public @ResponseBody List<String> findAllMetrics( @PathVariable final String name )
        throws IOException
    {
        return deleteMetric( name, true, true );
    }

    /**
     * Imports metric data points into a specific database.
     */
    @RequestMapping( value = "/dbloader/{db}/{metric:.+}", method = RequestMethod.POST )
    public @ResponseBody DataPointImportResults importSeriesData( @PathVariable final String db,
                                                                  @PathVariable final String metric,
                                                                  @RequestParam( name = "points", required = true ) String data,
                                                                  @RequestParam( name = "maxImportErrors", required = false, defaultValue = "0" ) int maxImportErrors,
                                                                  @RequestParam( name = "createMetricIfMissing", required = false, defaultValue = "false" ) boolean createMetricIfMissing )
        throws IOException
    {
        List<String> dbs = Arrays.asList( "60s24h", "5m7d", "30m2y" );
        if ( !dbs.contains( db ) )
        {
            throw new RuntimeException( String.format( "Unknown database [%s]", db ) );
        }

        final Metric m = tsStore().getMetric( metric, createMetricIfMissing );

        if ( m == null )
        {
            throw new RuntimeException( String.format( "Metric not found. Metric name: [%s]", metric ) );
        }

        List<DataPoint> points =
            Splitter.on( '\n' ).omitEmptyStrings().splitToList( data ).stream().map( line -> toDataPoint( m, line ) )
                .collect( Collectors.toList() );

        DataPointImportResults r = tsStore().importPoints( db, points, maxImportErrors );
        if ( log.isDebugEnabled() )
        {
            log.debug( "Import results: %s" + r );
        }
        return r;
    }

    private DataPoint toDataPoint( Metric m, String line )
    {
        String[] parts = line.split( "," );
        if ( parts.length != 2 )
        {
            throw new RuntimeException( "Invalid import line format: [" + line + "]" );
        }
        DataPoint p = new DataPoint( m.name, Double.parseDouble( parts[1] ), Integer.parseInt( parts[0] ) );
        p.setMetricId( m.id );
        return p;
    }
}
