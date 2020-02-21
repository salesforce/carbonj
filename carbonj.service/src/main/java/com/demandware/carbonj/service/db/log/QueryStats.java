/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.log;

import java.util.LongSummaryStatistics;

public class QueryStats {
    private final int totalNoOfDataPoints;
    private final LongSummaryStatistics waitTimeMillis;
    private final LongSummaryStatistics seriesReadTimeMillis;
    private final LongSummaryStatistics seriesWriteTimeMillis;
    private final LongSummaryStatistics emptySeriesReadTimeMillis;

    public QueryStats(int totalNoOfDataPoints, LongSummaryStatistics waitTimeMillis, LongSummaryStatistics seriesReadTimeMillis,
                      LongSummaryStatistics seriesWriteTimeMillis,
                      LongSummaryStatistics emptySeriesReadTimeMillis) {
        this.totalNoOfDataPoints = totalNoOfDataPoints;
        this.waitTimeMillis = waitTimeMillis;
        this.seriesReadTimeMillis = seriesReadTimeMillis;
        this.seriesWriteTimeMillis = seriesWriteTimeMillis;
        this.emptySeriesReadTimeMillis = emptySeriesReadTimeMillis;
    }

    public QueryStats() {
        totalNoOfDataPoints = 0;
        this.waitTimeMillis = new LongSummaryStatistics();
        this.seriesReadTimeMillis = new LongSummaryStatistics();
        this.seriesWriteTimeMillis = new LongSummaryStatistics();
        this.emptySeriesReadTimeMillis = new LongSummaryStatistics();
    }

    public LongSummaryStatistics getWaitTimeMillis() {
        return waitTimeMillis;
    }

    public LongSummaryStatistics getSeriesReadTimeMillis() {
        return seriesReadTimeMillis;
    }

    public LongSummaryStatistics getSeriesWriteTimeMillis() {
        return seriesWriteTimeMillis;
    }

    public LongSummaryStatistics getEmptySeriesReadTimeMillis() {
        return emptySeriesReadTimeMillis;
    }

    public int getTotalNoOfDataPoints() {
        return totalNoOfDataPoints;
    }
}
