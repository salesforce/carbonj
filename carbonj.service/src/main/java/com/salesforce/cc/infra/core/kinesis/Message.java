/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis;

import java.util.HashMap;
import java.util.Map;

public class Message {
    private final Map<String, String> headers;
    private final byte[] payload;

    public Message(Map<String, String> headers, byte[] payload) {
        this.headers = headers;
        this.payload = payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getHeader(String header) {
        return headers.get(header);
    }

    public String getHeader(String header, String defaultValue) {
        String value = getHeader(header);
        return (value == null) ? defaultValue: value;
    }

    public static class Builder {

        private final byte[] payload;
        private Map<String, String> headers;

        public Builder(byte[] payload) {
            this.payload = payload;
            this.headers = new HashMap<>();
        }

        public Builder addHeader(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public Message build() {
            return new Message(headers, payload);
        }
    }
}
