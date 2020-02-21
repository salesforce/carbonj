/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class ParserFactory {

    public static Parser getParser() {
        return getParser("1.0");
    }

    public static Parser getParser(String version) {
        switch(version) {
            case "0":
                return new DeprecatedParser();
            case "1.0":
                return new ParserV1();
            default:
                throw new RuntimeException("Unsupported version for parser: " + version);
        }
    }

    public static Parser getParser(byte[] message) throws IOException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(message);
        BufferedReader br = new BufferedReader(new InputStreamReader(byteIn));

        String versionLine = br.readLine();
        String[] arr = versionLine.split("/");

        if(arr[0].equals(AbstractParser.PROTOCOL_NAME)) {
            switch(arr[1].split("\n")[0]) {
                case "1.0":
                    return new ParserV1();
                default:
                    throw new RuntimeException("Unsupported versionLine for protocol");
            }
        } else {
            return new DeprecatedParser();
        }
    }
}
