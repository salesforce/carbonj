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
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import net.razorvine.pickle.Unpickler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Handles a server-side channel.
 */
public class PickleProtocolHandler
    extends ChannelInboundHandlerAdapter
{
    private final static Logger log = LoggerFactory.getLogger( PickleProtocolHandler.class );

    private static Counter invalids;

    private static Counter received;

    final Consumer<DataPoint> consumer;

    PickleProtocolHandler( MetricRegistry metricRegistry, Consumer<DataPoint> consumer )
    {
        this.consumer = Preconditions.checkNotNull( consumer );
        invalids = metricRegistry.counter(
                MetricRegistry.name( "pickleprotocol", "invalids" ) );

        received = metricRegistry.counter(
                MetricRegistry.name( "pickleprotocol", "received" ) );

    }

    static DataPoint decodePickle( Object m )
    {
        Object[] arr = (Object[]) m;
        if ( 2 != arr.length )
        {
            log.error( "Unexpected length of top level array -> skip" );
            return null;
        }
        String name = (String) arr[0];
        arr = (Object[]) arr[1];
        if ( arr.length != 2 )
        {
            log.error( "Unexpected length of data array- multiple datapoints per metric? array -> skip" );
            return null;
        }

        try {
            Number ts = (Number) arr[0];
            double val = getValue(arr[1]);
            return new DataPoint(name, val, ts.intValue());
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Invalid data point: %s %s", name, Arrays.toString(arr)), e);
            }
            return null;
        }
    }

    private static double getValue(Object valObj) {
        if (valObj instanceof String) {
            return Double.valueOf((String)valObj);
        } else if (valObj instanceof Number) {
            return ((Number)valObj).doubleValue();
        } else {
            throw new NumberFormatException();
        }
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        ByteBuf in = (ByteBuf) msg;
        try
        {
            in.readInt(); // skip first 4 bytes
            ByteBufInputStream is = new ByteBufInputStream( in );
            handle(is);
        }
        catch ( Exception e )
        {
            log.error( "error reading data " + ctx, e );
            throw Throwables.propagate( e );
        }
        finally
        {
            ReferenceCountUtil.release( msg );
        }
    }

    void handle(InputStream is) throws IOException {
        List data = (List) new Unpickler().load( is );
        received.inc(data.size());
        for ( Object o : data )
        {
            DataPoint dp = decodePickle( o );
            if ( null == dp )
            {
                invalids.inc();
                continue;
            }
            consumer.accept( dp );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        log.debug( "error in channel handler -> close context", cause );
        ctx.close();
    }
}
