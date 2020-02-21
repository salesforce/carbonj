/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import java.util.LongSummaryStatistics;

class BatchStats {

    final long waitTimeMillis;
    final int noOfDataPoints;
    final LongSummaryStatistics seriesReadTimeMillis;
    final LongSummaryStatistics seriesWriteTimeMillis;
    final LongSummaryStatistics emptySeriesReadTimeMillis;

    BatchStats(long waitTimeMillis, int noOfDataPoints, LongSummaryStatistics seriesReadTimeMillis,
               LongSummaryStatistics seriesWriteTimeMillis,
               LongSummaryStatistics emptySeriesReadTimeMillis) {
        this.waitTimeMillis = waitTimeMillis;
        this.noOfDataPoints = noOfDataPoints;
        this.seriesReadTimeMillis = seriesReadTimeMillis;
        this.seriesWriteTimeMillis = seriesWriteTimeMillis;
        this.emptySeriesReadTimeMillis = emptySeriesReadTimeMillis;
    }
}
