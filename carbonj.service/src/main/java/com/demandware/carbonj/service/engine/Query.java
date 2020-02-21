/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

public class Query {
    private final String pattern;
    private final int from;
    private final int to;
    private final int now;
    private final long receivedTimeInMillis;

    public Query(String pattern, int from, int to, int now, long receivedTimeInMillis) {
        this.pattern = pattern;
        this.from = from;
        this.to = to;
        this.now = now;
        this.receivedTimeInMillis = receivedTimeInMillis;
    }

    public String pattern() {
        return pattern;
    }

    public int from() {
        return from;
    }

    public int until() {
        return to;
    }

    public long receivedTimeInMillis() {
        return receivedTimeInMillis;
    }

    public int now() {
        return now;
    }

    @Override
    public String toString() {
        return "QueryContext{" +
                "query='" + pattern + '\'' +
                ", from=" + from +
                ", to=" + to +
                ", now=" + now +
                ", receivedTimeInMillis=" + receivedTimeInMillis +
                '}';
    }
}
