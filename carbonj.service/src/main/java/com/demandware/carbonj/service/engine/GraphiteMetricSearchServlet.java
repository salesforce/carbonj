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

import com.google.common.base.Preconditions;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.demandware.carbonj.service.events.EventsLogger;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MsgPackMetric;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Servlet to serve metrics metadata to graphite.
 */
public class GraphiteMetricSearchServlet
                extends HttpServlet
{
    private Logger LOG = LoggerFactory.getLogger( GraphiteMetricSearchServlet.class );

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
        LOG.info( GraphiteMetricSearchServlet.class.getSimpleName() + " initialized." );
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
        String query = req.getParameter( "query" );
        Preconditions.checkNotNull(query);
        boolean randomTest = req.getParameter("randomTest") != null;

        boolean msgpack = "msgpack".equals( format );
        boolean json = "json".equals(format);
        if( json )
        {
            res.setContentType( "application/json" );
        }
        else if ( msgpack )
        {
            res.setContentType("application/octet-stream");
        }
        else
        {
            res.setContentType( "application/pickle" );
        }
        res.setHeader("Cache-Control","no-cache");
        res.setHeader("Pragma","no-cache");

        List<Metric> metrics;

        if( randomTest )
        {
            metrics = new ArrayList<>();
            metrics.add( store.selectRandomMetric() );
        }
        else
        {
            if( queryBlacklist.match(query) )
            {
                String msg = String.format("Query pattern [%s] is blacklisted.", query);
                LOG.warn(msg);

                logger.log(new BlackListedQueryEvent(query));
                throw new RuntimeException(msg);
            }

            metrics = store.findMetrics( query, false, true, true );
        }

        if( json )
        {
            Gson gson = new Gson();
            res.getWriter().write( gson.toJson( metrics ) );
            res.getWriter().close();
        }
        else if ( msgpack )
        {
            ObjectMapper objectMapper = new ObjectMapper( new MessagePackFactory() );

            List<MsgPackMetric> msgPackMetrics = new ArrayList<>();

            for ( Metric metric : metrics )
            {
                msgPackMetrics.add( new MsgPackMetric( metric ) );
            }

            try ( OutputStream output = res.getOutputStream() )
            {
                // Serialize the metrics
                byte[] serialized = objectMapper.writeValueAsBytes( msgPackMetrics );
                res.setContentLength( serialized.length );
                output.write( serialized );

            }
            catch ( IOException e )
            {
                LOG.error( "carbonapi request: error writing msgpack response", e.getMessage() );
            }
        }
        else
        {
            new GraphitePickler().pickleMetrics( metrics, res.getOutputStream() );
        }

    }

}
