/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.util;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class UdpServer {

    private final DatagramSocket socket;
    private final int size;

    public UdpServer(int port, int size) throws Exception {
        socket = new DatagramSocket(port);
        this.size = size;
    }

    public void serve() throws Exception {
        while (true) {
            byte[] buf = new byte[size];
            DatagramPacket packet
                    = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            System.out.println("Payload Size: " + buf.length);
            String message = new String(buf, StandardCharsets.US_ASCII);
            System.out.print("Time: " + new Date() + "Message: " + message);
        }
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int payLoadSize = Integer.parseInt(args[1]);
        new UdpServer(port, payLoadSize).serve();
    }
}
