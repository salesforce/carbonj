/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;

import java.util.LinkedList;
import java.util.List;

public class ReservedHeaders {
    public enum reserved {
        PayloadLength,
    }

    public static List<String> getReservedHeaders() {
        List<String> list = new LinkedList<>();

        for(reserved r : reserved.values()) {
            list.add(String.valueOf(r));
        }

        return list;
    }
}
