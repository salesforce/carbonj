/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.util;

import java.io.*;
import java.net.*;
import java.util.Date;

class TcpServer {

    public static void main(String argv[]) throws Exception {
        ServerSocket welcomeSocket = new ServerSocket(5697);

        while (true) {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient =
                    new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));

            System.out.println("Time: " + new Date());
            try {
                String clientSentence = inFromClient.readLine();
                while (clientSentence != null) {
                    System.out.println("Received: " + clientSentence);
                    clientSentence = inFromClient.readLine();
                }
            } finally {
                // inFromClient.close();
                // connectionSocket.getOutputStream().close();
                connectionSocket.close();
            }
        }
    }

    private static void closeQuitely() {

    }
}
