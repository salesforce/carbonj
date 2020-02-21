/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.log;

import com.demandware.carbonj.service.engine.Query;
import com.demandware.carbonj.service.events.CarbonjEvent;
import com.demandware.carbonj.service.events.Constants;

public class CompletedQueryStats extends Stats implements CarbonjEvent {
    final long from;
    final long to;
    final boolean completed;
    final boolean heavyQuery;
    final QueryStats queryStats;

    public CompletedQueryStats(Query query, int noOfSeries, boolean heavyQuery,
                               long time, QueryStats queryStats) {
        super(Constants.QUERY_LOG_TYPE, "render", query.pattern(), time - query.receivedTimeInMillis(), noOfSeries, time);
        this.from = query.from();
        this.to = query.until();
        this.queryStats = queryStats;
        this.completed = true;
        this.heavyQuery = heavyQuery;
    }
}
