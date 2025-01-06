/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import net.razorvine.pickle.Pickler;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPickleProtocolHandler {
    @Test
    public void test() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        TestConsumer testConsumer = new TestConsumer();
        PickleProtocolHandler pickleProtocolHandler = new PickleProtocolHandler(metricRegistry, testConsumer);
        Pickler pickler = new Pickler();
        List<Object[]> objectList = new ArrayList<>();
        Object[] objects = new Object[1];
        objects[0] = "foo.bar";
        objectList.add(objects);
        byte[] bytes = pickler.dumps(objectList);
        byte[] updateBytes = new byte[bytes.length + 4];
        System.arraycopy(bytes, 0, updateBytes, 4, bytes.length);
        pickleProtocolHandler.channelRead(null, Unpooled.wrappedBuffer(updateBytes));

        objectList.clear();
        objects = new Object[2];
        objects[0] = "foo.bar";
        objects[1] = new Object[1];
        ((Object[])objects[1])[0] = (int) (System.currentTimeMillis() / 1000);
        objectList.add(objects);
        bytes = pickler.dumps(objectList);
        updateBytes = new byte[bytes.length + 4];
        System.arraycopy(bytes, 0, updateBytes, 4, bytes.length);
        pickleProtocolHandler.channelRead(null, Unpooled.wrappedBuffer(updateBytes));
        assertEquals(0, testConsumer.count);

        objectList.clear();
        objects = new Object[2];
        objects[0] = "foo.bar";
        objects[1] = new Object[2];
        ((Object[])objects[1])[0] = (int) (System.currentTimeMillis() / 1000);
        ((Object[])objects[1])[1] = 123.45;
        objectList.add(objects);
        bytes = pickler.dumps(objectList);
        updateBytes = new byte[bytes.length + 4];
        System.arraycopy(bytes, 0, updateBytes, 4, bytes.length);
        pickleProtocolHandler.channelRead(null, Unpooled.wrappedBuffer(updateBytes));
        assertEquals(1, testConsumer.count);

        objectList.clear();
        objects = new Object[2];
        objects[0] = "foo.bar";
        objects[1] = new Object[2];
        ((Object[])objects[1])[0] = (int) (System.currentTimeMillis() / 1000);
        ((Object[])objects[1])[1] = "123#45";
        objectList.add(objects);
        bytes = pickler.dumps(objectList);
        updateBytes = new byte[bytes.length + 4];
        System.arraycopy(bytes, 0, updateBytes, 4, bytes.length);
        pickleProtocolHandler.channelRead(null, Unpooled.wrappedBuffer(updateBytes));

        pickleProtocolHandler.exceptionCaught(new TestChannelHandlerContext(), new RuntimeException("test"));
    }

    private static class TestConsumer implements Consumer<DataPoint> {
        private int count = 0;

        @Override
        public void accept(DataPoint dataPoint) {
            count++;
        }
    }

    private static class TestChannelHandlerContext implements ChannelHandlerContext {

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public EventExecutor executor() {
            return null;
        }

        @Override
        public String name() {
            return "";
        }

        @Override
        public ChannelHandler handler() {
            return null;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelActive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught(Throwable throwable) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered(Object o) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelRead(Object o) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress socketAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelHandlerContext read() {
            return null;
        }

        @Override
        public ChannelFuture write(Object o) {
            return null;
        }

        @Override
        public ChannelFuture write(Object o, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelHandlerContext flush() {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object o) {
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable throwable) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return null;
        }

        @Override
        public <T> Attribute<T> attr(AttributeKey<T> attributeKey) {
            return null;
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
            return false;
        }
    }
}
