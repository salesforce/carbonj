/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Servlet to serve series data to graphite.
 */
public class GraphiteSeriesDataTestServlet
                extends HttpServlet
{
    private Logger LOG = LoggerFactory.getLogger( GraphiteSeriesDataTestServlet.class );

    @Autowired(required = false)
    private TimeSeriesStore store;

    private TimeSource timeSource = TimeSource.defaultTimeSource();

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
        LOG.info( GraphiteSeriesDataTestServlet.class.getSimpleName() + " initialized." );
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

        int count = Integer.parseInt(req.getParameter("count"));

        int now = timeSource.getEpochSecond() - 60;
        int until = now;
        int from = until - 24 * 60 * 60 + 120;

        long start = System.currentTimeMillis();
        int nPoints = 0;
        List<String> names = new ArrayList<>(  );
        for(int i = 0; i < count; i++)
        {
            Metric m = store.selectRandomMetric();
            List<Series> series = store.fetchSeriesData( new Query(m.name, from, until, now, System.currentTimeMillis()) );
            for(Series s : series)
            {
                nPoints = nPoints + s.values.size();
                names.add( s.name );
            }
        }

        long time = System.currentTimeMillis() - start;

        Summary summary = new Summary();
        summary.setElapsedTimeMillis( time );
        summary.setTotalPoints( nPoints );
        summary.setFrom(from);
        summary.setUntil(until);
        summary.setMetricNames( names );

        res.setContentType( "application/json" );
        res.setHeader("Cache-Control","no-cache");
        res.setHeader("Pragma","no-cache");

        Gson gson = new Gson();
        res.getWriter().write( gson.toJson( summary ) );
        res.getWriter().close();
    }

    static class Summary
    {
        private long elapsedTimeMillis;
        private int totalPoints;
        private List<String> metricNames;
        private long from;
        private long until;

        public long getElapsedTimeMillis()
        {
            return elapsedTimeMillis;
        }

        public void setElapsedTimeMillis( long elapsedTimeMillis )
        {
            this.elapsedTimeMillis = elapsedTimeMillis;
        }

        public int getTotalPoints()
        {
            return totalPoints;
        }

        public void setTotalPoints( int totalPoints )
        {
            this.totalPoints = totalPoints;
        }

        public List<String> getMetricNames()
        {
            return metricNames;
        }

        public void setMetricNames( List<String> metricNames )
        {
            this.metricNames = metricNames;
        }

        public long getFrom()
        {
            return from;
        }

        public void setFrom( long from )
        {
            this.from = from;
        }

        public long getUntil()
        {
            return until;
        }

        public void setUntil( long until )
        {
            this.until = until;
        }
    }
}
