/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.*;

public class TestLineProtocolDestinationsService {
    @Test
    public void test() throws Exception {
        DataPoint dataPoint = new DataPoint("foo.bar", 123, (int) (System.currentTimeMillis() / 1000));
        ServerSocket serverSocket = new ServerSocket(0);
        Thread thread = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String line = reader.readLine();
                assertNotNull(line);
                String[] parts = line.split(" ");
                assertEquals(dataPoint.name, parts[0]);
                assertEquals(dataPoint.val, Double.parseDouble(parts[1]));
                assertEquals(dataPoint.ts, Integer.parseInt(parts[2]));
            } catch (IOException e) {
                fail(e.getMessage());
            }
        });
        thread.start();
        int port = serverSocket.getLocalPort();
        MetricRegistry metricRegistry = new MetricRegistry();
        LineProtocolDestinationsService lineProtocolDestinationsService =
                new LineProtocolDestinationsService(metricRegistry, "relay", "127.0.0.1", port, 1, 1, 100);
        assertEquals("dest.relay.127.0.0.1", lineProtocolDestinationsService.name);
        assertTrue(lineProtocolDestinationsService.toString().startsWith("Thread["));
        LineProtocolDestination[] lineProtocolDestinations = lineProtocolDestinationsService.getDestinations();
        assertEquals(1, lineProtocolDestinations.length);
        lineProtocolDestinationsService.accept(dataPoint);
        Thread.sleep(500);
        lineProtocolDestinationsService.closeQuietly();
        serverSocket.close();
        thread.join();
        LineProtocolDestinationSocket lineProtocolDestinationSocket = (LineProtocolDestinationSocket) lineProtocolDestinations[0];
        checkMeter(metricRegistry, lineProtocolDestinationSocket.name + ".recv", 1L);
        checkMeter(metricRegistry, lineProtocolDestinationSocket.name + ".sent", 1L);
    }

    private void checkMeter(MetricRegistry metricRegistry, String name, long expected) {
        Meter meter = metricRegistry.getMeters().get(name);
        assertNotNull(meter);
        assertEquals(expected, meter.getCount());
    }
}
