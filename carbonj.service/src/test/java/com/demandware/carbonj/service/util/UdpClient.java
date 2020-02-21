/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.util;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class UdpClient {

    private static final String host = "localhost";
    private static final int port = 8000;
    private static final int metricsPerMinute = 40000;
    private static final int size = 40;

    private void execute() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

        int noOfTasks = metricsPerMinute / size;
        try {
            while (true) {
                for (int i = 0; i <= noOfTasks; i++) {
                    SendMetricsTask task = new SendMetricsTask(i, size, host, port);
                    executor.execute(task);
                }
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            }
        } finally {
            executor.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        new UdpClient().execute();
    }

    private static class SendMetricsTask implements Runnable {

        private final int taskNo;
        private final int batchSize;
        private final String host;
        private final int port;

        SendMetricsTask(int taskNo, int batchSize, String host, int port) {

            this.taskNo = taskNo;
            this.batchSize = batchSize;
            this.host = host;
            this.port = port;
        }

        public void run() {
            try {
                String msg = generateMessage(batchSize, taskNo);
                // System.out.println("Message: " + msg);
                byte[] buf = msg.getBytes(StandardCharsets.UTF_8);
                System.out.println("Payload Size: " + buf.length + " Date: " + new Date());
                try (DatagramSocket socket = new DatagramSocket()) {
                    InetAddress address = InetAddress.getByName(host);
                    DatagramPacket packet
                            = new DatagramPacket(buf, buf.length, address, port);
                    socket.send(packet);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private String generateMessage(int size, int taskNo) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < size; i++) {
                String dataPoint = String.format("pod5.ecom.bbmv.bbmv_stg.blade1-5_mon_demandware_net.bbmv_stg.debug.scripting.%d.%d.count 5 %d\n", i, taskNo, Instant.now().getEpochSecond());
                stringBuilder.append(dataPoint);
            }
            return stringBuilder.toString();
        }

    }
}
