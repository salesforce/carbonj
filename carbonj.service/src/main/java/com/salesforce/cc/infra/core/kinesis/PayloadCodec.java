/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis;


import com.salesforce.cc.infra.core.kinesis.parser.Parser;
import com.salesforce.cc.infra.core.kinesis.parser.ParserFactory;

public class PayloadCodec {

    public static byte[] encode(Message message) throws Exception {
        Parser parser = ParserFactory.getParser();
        return parser.encode(message);
    }

    public static Message decode(byte[] payload) throws Exception {
        Parser parser = ParserFactory.getParser(payload);
        return parser.decode(payload);
    }
}
