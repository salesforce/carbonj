/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Handles a server-side channel.
 */
public class LineProtocolHandler
    extends ChannelInboundHandlerAdapter
{
    private final static Logger log = LoggerFactory.getLogger( LineProtocolHandler.class );

    private static Counter invalidDataPoints;

    private final static String NEW_LINE_STR = System.getProperty("line.separator");

    final Consumer<DataPoint> consumer;

    LineProtocolHandler( MetricRegistry metricRegistry, Consumer<DataPoint> consumer )
    {
        this.consumer = Preconditions.checkNotNull( consumer );
        invalidDataPoints = metricRegistry.counter(
                MetricRegistry.name( "lineprotocol", "invalids" ) );


    }

    public static DataPoint parse( String line )
    {
        String[] parts = StringUtils.split( line, ' ' );

        if ( parts.length != 3 )
        {
            if (log.isDebugEnabled()) {
                log.debug( "bad format: " + line );
            }
            invalidDataPoints.inc();
            return null;
        }

        try {
            double value = Double.parseDouble(parts[1]);
            int epochInSecs = Integer.parseInt(parts[2]);
            return new DataPoint( parts[0], value, epochInSecs);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Invalid data point: " + line, e);
            }
            invalidDataPoints.inc();
            return null;
        }
    }

    private static void parse( ByteBuf buff, Consumer<DataPoint> dp )
    {
        String m = buff.toString( io.netty.util.CharsetUtil.US_ASCII );
        String[] metrics = m.split(NEW_LINE_STR);

        for ( String metric : metrics )
        {
            metric = metric.trim();

            DataPoint dataPoint = parse(metric);
            if (dataPoint != null) {
                dp.accept(dataPoint);
            }
        }
    }

    public void process( ByteBuf in )
    {
        parse( in, consumer );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        try
        {
            process( (ByteBuf) msg );
        }
        finally
        {
            ReferenceCountUtil.release( msg );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        log.debug( "error in channel handler -> close context", cause );
        ctx.close();
    }
}
