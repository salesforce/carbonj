/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.util;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Disabled
public class TestStatsd {
    
    @Test
    public void testStatsd() throws Exception {
        int counter = 0;
        String name ="test.sum.counter";
        while (true) {
            counter++;
            sendMetrics(name, counter);
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));
        }
    }

    private void sendMetrics(String name, int counter) {
        try {
            String msg = String.format("%s:%s|c", name, counter);
            byte[] buf = msg.getBytes(StandardCharsets.UTF_8);
            System.out.println("msg: " + msg + " Date: " + new Date());
            try (DatagramSocket socket = new DatagramSocket()) {
                InetAddress address = InetAddress.getByName("localhost");
                DatagramPacket packet
                        = new DatagramPacket(buf, buf.length, address, 8125);
                socket.send(packet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
