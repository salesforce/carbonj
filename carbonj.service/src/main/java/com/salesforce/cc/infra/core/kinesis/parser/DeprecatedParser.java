/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;

import com.salesforce.cc.infra.core.kinesis.Message;

import java.util.HashMap;

class DeprecatedParser extends AbstractParser {

    DeprecatedParser() {
        super("0");
    }

    @Override
    public byte[] encode(Message message) {
        throw new UnsupportedOperationException("Deprecated");
    }

    @Override
    public Message decode(byte[] payload) {
        return new Message(new HashMap<>(), payload);
    }
}
