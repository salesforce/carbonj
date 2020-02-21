/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.events.CarbonjEvent;

class BlackListedQueryEvent implements CarbonjEvent {
    private final String target;
    private String from;
    private String until;
    private String now;
    private final String type = "blacklisted";
    private final long time;

    public BlackListedQueryEvent(String target, String from, String until, String now) {
        this.target = target;
        this.from = from;
        this.until = until;
        this.now = now;
        time = System.currentTimeMillis();
    }

    public BlackListedQueryEvent(String target) {
        this.target = target;
        time = System.currentTimeMillis();
    }

    public String getTarget() {
        return target;
    }

    public String getFrom() {
        return from;
    }

    public String getUntil() {
        return until;
    }

    public String getNow() {
        return now;
    }

    public String getType() {
        return type;
    }

    public long getTime() {
        return time;
    }
}
