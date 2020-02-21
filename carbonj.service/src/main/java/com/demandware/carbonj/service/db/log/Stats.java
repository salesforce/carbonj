/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.log;

public abstract class Stats {

    final String query;
    final long responseTime;
    final int noOfSeries;
    final long time;
    final String queryType;
    final String type;

    public Stats(String type, String queryType, String query, long responseTime, int noOfMetrics, long time) {
        this.type = type;
        this.queryType = queryType;
        this.query = query;
        this.responseTime = responseTime;
        this.noOfSeries = noOfMetrics;
        this.time = time;
    }
}
