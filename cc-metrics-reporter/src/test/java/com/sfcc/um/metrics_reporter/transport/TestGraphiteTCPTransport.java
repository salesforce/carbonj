/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.transport;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class TestGraphiteTCPTransport {
    @Test
    public void test() throws Exception {
        String name = "foo.bar";
        String value = "123.45";
        int current = (int) (System.currentTimeMillis() / 1000);
        ServerSocket serverSocket = new ServerSocket(0);
        Thread thread = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line = reader.readLine();
                assertNotNull(line);
                String[] parts = line.split(" ");
                assertEquals(name, parts[0]);
                assertEquals(value, parts[1]);
                assertEquals(current, Integer.parseInt(parts[2]));
            } catch (IOException e) {
                fail(e.getMessage());
            }
        });
        thread.start();
        int port = serverSocket.getLocalPort();
        MetricRegistry metricRegistry = new MetricRegistry();
        GraphiteTCPTransport graphiteTCPTransport = new GraphiteTCPTransport("127.0.0.1", port, 1, metricRegistry);
        graphiteTCPTransport.open();
        graphiteTCPTransport.send(name, value, current);
        graphiteTCPTransport.close();
        serverSocket.close();
        thread.join();
    }
}
