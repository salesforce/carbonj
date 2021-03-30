/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.cfgCarbonJ;
import com.demandware.carbonj.service.engine.netty.NettyChannel;
import com.demandware.carbonj.service.engine.netty.NettyServer;
import io.netty.channel.*;
import io.netty.channel.socket.DatagramPacket;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { CfgKinesisEventsLogger.class } )
public class cfgEventBus {

    @Value("${events.port:2015}" )
    private int eventsPort;

    @Value("${events.host:0.0.0.0}" )
    private String eventsHost;

    @Value( "${events.udp.buff:1048576}" )
    private int udpBuff;

    @Value( "${events.udp.msgbuff:8192}" )
    private int udpMsgBuff;

    @Autowired
    private MetricRegistry metricRegistry;

    @Bean
    NettyChannel eventBus(NettyServer netty, @Qualifier("KinesisEventsLogger") EventsLogger<byte[]> eventsLogger)
    {
        NettyChannel channel = netty.udpBind( eventsHost, eventsPort, udpBuff, udpMsgBuff,
                new SimpleChannelInboundHandler<DatagramPacket>()
        {
            EventsHandler eventsHandler = new EventsHandlerImpl( eventsLogger );

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg )
            {
                // temporary metric used to correlate number of messages sent with number of messages received.
                metricRegistry.meter( MetricRegistry.name( "events", "udpMsgsReceived" ) ).mark();

                eventsHandler.process( msg.content() );
            }
        } );

        channel.checkUdpChannelConfig( udpBuff, udpMsgBuff );

        return channel;
    }
}
