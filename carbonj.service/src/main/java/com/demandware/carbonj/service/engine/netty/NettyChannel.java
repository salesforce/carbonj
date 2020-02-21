/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.netty;

import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;

import javax.annotation.PreDestroy;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.channel.ChannelOption;
import io.netty.channel.RecvByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyChannel
{
    private static final Logger log = LoggerFactory.getLogger(NettyChannel.class);
    private final ChannelFuture f;

    public NettyChannel( ChannelFuture f )
    {
        this.f = Preconditions.checkNotNull( f );
        f.syncUninterruptibly();
        if ( !f.isSuccess() )
        {
            throw Throwables.propagate( f.cause() );
        }
    }

    @PreDestroy
    public void close()
    {
        f.channel().close().syncUninterruptibly();
    }

    public ChannelFuture getChannelFuture() {
        return f;
    }

    public void checkUdpChannelConfig( int udpBuff, int udpMsgBuff)
    {
        ChannelConfig config = f.channel().config();
        int ACTUAL_SO_RCVBUF = config.getOption( ChannelOption.SO_RCVBUF );
        if ( ACTUAL_SO_RCVBUF != udpBuff )
        {
            log.error( "UDP configuration error. Configured UDP SO_RCVBUF=" + udpBuff + ", actual SO_RCVBUF="
                    + ACTUAL_SO_RCVBUF + ", check /proc/sys/net/core/rmem_max" );
        }
        if ( udpMsgBuff > udpBuff / 10 ) // 10 is quite an arbitrary number here.
        {
            log.error( "UDP configuration error. udpMsgBuff=" + udpMsgBuff + ", udpBuff=" + udpBuff
                    + ". This config can only buffer " + ( udpBuff / udpMsgBuff ) + " messages." );
        }

        RecvByteBufAllocator recvByteBufAllocator = config.getOption(ChannelOption.RCVBUF_ALLOCATOR);
        int receiveBufferSize = recvByteBufAllocator.newHandle().guess();
        if (receiveBufferSize != udpMsgBuff) {
            log.error( "UDP configuration error. Configured UDP Receive Buffer = " + udpMsgBuff + ", actual = "
                    + receiveBufferSize );
        } else {
            log.info("Configured UDP Receive Buffer = " + udpMsgBuff);
        }
    }
}
