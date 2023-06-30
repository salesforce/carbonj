/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.demandware.carbonj.service.events.EventsLogger;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.SystemTime;
import com.demandware.carbonj.service.engine.protobuf.MetricsResponse;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Servlet to serve series data to graphite.
 */
public class GraphiteSeriesDataServlet
                extends HttpServlet
{
    private Logger LOG = LoggerFactory.getLogger( GraphiteSeriesDataServlet.class );

    @Autowired(required = false)
    private TimeSeriesStore store;

    @Autowired
    @Qualifier("queryBlacklist")
    private MetricList queryBlacklist;

    @Autowired(required = false)
    @Qualifier("CarbonjEventsLogger")
    EventsLogger logger;

    @Override
    public void init( ServletConfig config )
                    throws ServletException
    {
        super.init( config );

        // wire spring beans referenced by this servlet
        WebApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext( config.getServletContext() );
        if ( ctx != null )
        {
            ctx.getAutowireCapableBeanFactory().autowireBean( this );
        }
        LOG.info( GraphiteSeriesDataServlet.class.getSimpleName() + " initialized." );
    }

    @Override
    public void destroy()
    {
    }

    @Override
    protected void doGet( HttpServletRequest req, HttpServletResponse res )
                    throws IOException
    {
        if( store == null )
        {
            throw new RuntimeException( "Time Series store is not configured." );
        }

        String format = req.getParameter("format");
        String target = req.getParameter( "target" );
        String from = req.getParameter( "from" );
        String until = req.getParameter( "until" );
        String nowText = req.getParameter("now");
        int now = SystemTime.nowEpochSecond();
        if( nowText != null )
        {
            now = Integer.parseInt( nowText );
        }

        boolean randomTest = req.getParameter("randomTest") != null;

        boolean protobuf = "carbonapi_v3_pb".equals( format );
        boolean json = "json".equals( format );
        if( json )
        {
            res.setContentType( "application/json" );
        }
        else
        {
            res.setContentType( "application/pickle" );
        }
        res.setHeader("Cache-Control","no-cache");
        res.setHeader("Pragma","no-cache");

        if( randomTest )
        {
            if( target == null )
            {
                target = store.selectRandomMetric().name;
            }

            if( from == null && until == null)
            {
                until = String.valueOf(now);
                from = String.valueOf(now - 24 * 60 * 60 + 120);
            }
        }

        if( target.equals("random") )
        {
            target = store.selectRandomMetric().name;
        }


        if( queryBlacklist.match(target) )
        {
            String msg = String.format("Query pattern [%s] is blacklisted.", target);
            LOG.warn(msg);

            logger.log(new BlackListedQueryEvent(target, from, until, nowText));
            throw new RuntimeException(msg);
        }

        if( json )
        {
            List<Series> series = store.fetchSeriesData( new Query(target, Integer.parseInt( from ),
                    Integer.parseInt( until ), now, System.currentTimeMillis()) );

            Gson gson = new Gson();
            res.getWriter().write( gson.toJson( series ) );
            res.getWriter().close();
        }
        else if ( protobuf )
        {
            List<Series> seriesList = store.fetchSeriesData( new Query( target, Integer.parseInt( from ),
                Integer.parseInt( until ), now, System.currentTimeMillis() ) );

            OutputStream output = res.getOutputStream();

            List<MetricsResponse.Series> metricsSeriesList = new ArrayList<MetricsResponse.Series>();
            for ( Series series : seriesList )
            {
                List<MetricsResponse.Value> valuesList = new ArrayList<MetricsResponse.Value>();
                for ( Double value : series.values )
                {
                    valuesList.add( MetricsResponse.Value.newBuilder().setValue( value ).build() );
                }
                MetricsResponse.Series metricsSeries =
                    MetricsResponse.Series.newBuilder().setName( series.name ).setStart( series.start )
                        .setEnd( series.end ).setStep( series.step ).addAllValues( valuesList ).build();

                metricsSeriesList.add( metricsSeries );
            }

            MetricsResponse.SeriesList response =
                MetricsResponse.SeriesList.newBuilder().addAllSeriesList( metricsSeriesList ).build();

            try
            {
                response.writeTo( output );
            }
            finally
            {
                output.close();
            }
        }
        else
        {
            ResponseStream seriesStream = new GraphitePickler( false, res.getOutputStream() );
            try
            {
                store.streamSeriesData(new Query(target, Integer.parseInt(from), Integer.parseInt(until), now, System.currentTimeMillis()), seriesStream);
            }
            finally
            {
                seriesStream.close();
            }
        }
    }

}