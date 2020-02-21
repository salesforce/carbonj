/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.Quota;
import com.demandware.carbonj.service.engine.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LatePointLoggerImpl implements LatePointLogger {
    private static Logger log = LoggerFactory.getLogger( LatePointLoggerImpl.class );
    private final Counter skippedDelayed;

    private final Quota latePointsLogQuota = new Quota(100, 60);

    private final Histogram aggregatorPointAge;

    public LatePointLoggerImpl(MetricRegistry metricRegistry)
    {
        this.skippedDelayed = metricRegistry.counter(
                MetricRegistry.name( "aggregator", "skippedDelayed" ) );
        this.aggregatorPointAge = metricRegistry.histogram(
            MetricRegistry.name("aggregator", "pointAgeHistogram"));
    }

    @Override
    public void logLatePoint(DataPoint m, long now, Reason r, String context)
    {
        // record point age only for skipped points to avoid overhead of updating histogram for every point
        long age = now - m.ts;
        aggregatorPointAge.update(age);

        skippedDelayed.inc();

        if( latePointsLogQuota.allow() )
        {
            log.warn( String.format(
                    "point skipped aggregation because it was received too late: age [%s], point: [%s]. reason: [%s] context: [%s]",
                    age, m, r, context) );
        }
    }
}
