/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;

import com.salesforce.cc.infra.core.kinesis.Message;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class ParserV1 extends AbstractParser {

    ParserV1() {
        super("1.0");
    }

    @Override
    public byte[] encode(Message message) throws Exception {

        //validate Headers
        validateHeaders(message.getHeaders());

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        //Write versionLine
        byteOut.write((this.versionLine + "\n").getBytes());

        writeReservedHeaders(message, byteOut);

        //Write Headers
        for(Map.Entry<String, String> header: message.getHeaders().entrySet()) {
            String headerLine = getHeaderString(header.getKey(), header.getValue());
            byteOut.write(headerLine.getBytes());
        }

        //Blank line between header and payload
        byteOut.write("\n".getBytes());

        //Write payload
        byteOut.write(message.getPayload());

        return byteOut.toByteArray();
    }

    @Override
    public Message decode(byte[] message) throws Exception {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(message);
        BufferedReader br = new BufferedReader(new InputStreamReader(byteIn));

        String line;

        //Parse HTTP versionLine
        br.readLine();

        //Parse Headers
        HashMap<String, String> map = new HashMap<>();
        while(!(line = br.readLine()).isEmpty()) {
            String[] header = line.split(":");
            map.put(header[0], header[1]);
        }

        //Store payload as a byte array
        int payLoadLength = Integer.parseInt(map.get(String.valueOf(ReservedHeaders.reserved.PayloadLength)));
        byte[] payload = Arrays.copyOfRange(message, message.length - payLoadLength, message.length);

        return new Message(map, payload);
    }

    private void validateHeaders(Map<String, String> headers) throws Exception {
        for(String header : ReservedHeaders.getReservedHeaders()) {
            if(headers.containsKey(header)) {
                throw new Exception("Header cannot be used by user as it is reserved by parser: " + header);
            }
        }
    }

    private void writeReservedHeaders(Message message, ByteArrayOutputStream byteOut) throws IOException {
        int payLoadLength = message.getPayload().length;
        byteOut.write(
                getHeaderString(
                        String.valueOf(ReservedHeaders.reserved.PayloadLength),
                        String.valueOf(payLoadLength)
                ).getBytes()
        );
    }

    private String getHeaderString(String key, String val) {
        return key + ":" + val + "\n";
    }
}
