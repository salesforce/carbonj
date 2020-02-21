/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EventsUdpClient {

    private static final String HOST = "localhost";
    private static final int PORT = 2015;
    private static final int EVENTS_PER_MINUTE = 10000;
    private static final int EVENTS_PER_PAYLOAD = 10;

    private static final String EVENT_PREFIX = "UMONTP/1.0\n" +
            "Domain:test\n" +
            "Payload-Version:1.0\n" +
            "\n";

    private static final String EVENT_START =        "[  \n";
    private static final String EVENT_TEMPLATE =         "   {  \n" +
            "      \"time\":%d,\n" +
            "      \"ipAddresses\":[  \n" +
            "         \"216.58.194.206\",\n" +
            "         \"2607:f8b0:4005:805:0:0:0:200e\"\n" +
            "      ],\n" +
            "      \"type\":\"dns\",\n" +
            "      \"taskNo\":%d,\n" +
            "      \"eventNo\":%d\n" +
            "   }\n";

    private static final String EVENT_END =  "]";

    private void execute() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

        int noOfTasks = EVENTS_PER_MINUTE / EVENTS_PER_PAYLOAD;
        try {
            while (true) {
                for (int i = 0; i < noOfTasks; i++) {
                    SendEventsTask task = new SendEventsTask(i, EVENTS_PER_PAYLOAD, HOST, PORT);
                    executor.execute(task);
                }
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            }
        } finally {
            executor.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        new EventsUdpClient().execute();
    }

    private static class SendEventsTask implements Runnable {

        private final int taskNo;
        private final int batchSize;
        private final String host;
        private final int port;

        SendEventsTask(int taskNo, int batchSize, String host, int port) {

            this.taskNo = taskNo;
            this.batchSize = batchSize;
            this.host = host;
            this.port = port;
        }

        public void run() {
            try {
                String msg = generateEvents(batchSize, taskNo);
                // System.out.println("Message: " + msg);
                byte[] buf = msg.getBytes(StandardCharsets.UTF_8);
                System.out.println("Payload Size: " + buf.length + " Date: " + new Date());
                try (DatagramSocket socket = new DatagramSocket()) {
                    InetAddress address = InetAddress.getByName(host);
                    DatagramPacket packet
                            = new DatagramPacket(buf, buf.length, address, port);
                    socket.send(packet);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String generateEvents(int size, int taskNo) {
            StringBuilder eventBuilder = new StringBuilder();
            eventBuilder.append(EVENT_PREFIX);
            eventBuilder.append(EVENT_START);
            String event = String.format(EVENT_TEMPLATE, System.currentTimeMillis(), taskNo, 0);
            eventBuilder.append(event);
            for (int i = 1; i < size; i++) {
                eventBuilder.append(",");
                event = String.format(EVENT_TEMPLATE, System.currentTimeMillis(), taskNo, i);
                eventBuilder.append(event);
            }
            eventBuilder.append(EVENT_END);
            return eventBuilder.toString();
        }
    }
}
