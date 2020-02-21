/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.netty;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer
{
    private static Logger log = LoggerFactory.getLogger( NettyServer.class );

    private final EventLoopGroup bossGroup;

    private final EventLoopGroup workerGroup;

    public static Meter udpMsgsReceived;


    public NettyServer( MetricRegistry metricRegistry, int ioThreads, int workersThreads )
    {
        udpMsgsReceived = metricRegistry.meter( MetricRegistry.name( "nettyserver", "udpMsgsReceived" ) );
        bossGroup = new NioEventLoopGroup( ioThreads );
        workerGroup = new NioEventLoopGroup( workersThreads );
    }

    @PreDestroy
    void shutdown()
    {
        log.info( "stopping" );
        bossGroup.shutdownGracefully().awaitUninterruptibly( 100 );
        workerGroup.shutdownGracefully().awaitUninterruptibly( 100 );
        log.info( "stopped" );
    }

    public NettyChannel bind( String host, int port, ChannelInitializer<SocketChannel> init )
    {
        log.info( String.format("listening on %s %d", host, port ));
        ServerBootstrap b = new ServerBootstrap();
        b.group( bossGroup, workerGroup ).channel( NioServerSocketChannel.class ).childHandler( init )
            .option( ChannelOption.SO_BACKLOG, 128 ).childOption( ChannelOption.SO_KEEPALIVE, true );
        return new NettyChannel( b.bind( host, port ) );
    }

    public NettyChannel udpBind( String host, int port, int socketBufSize, int messageBufSize, ChannelHandler handler )
    {
        log.info( String.format("listening on UDP %s %d", host, port ));
        Bootstrap b = new Bootstrap();
        b.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator( messageBufSize ))
            .option(ChannelOption.SO_RCVBUF, socketBufSize);
        b.group( bossGroup ).channel( NioDatagramChannel.class ).handler( handler );
        return new NettyChannel( b.bind( host, port ) );
    }

    public void dumpStats()
    {
        log.info( "received udp msgs: " + udpMsgsReceived.getCount());
    }
}
