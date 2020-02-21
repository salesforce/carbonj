/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator.recovery;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.LatePointLogger;
import com.demandware.carbonj.service.db.util.Quota;
import com.demandware.carbonj.service.engine.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryLatePointLogger implements LatePointLogger {
    private final MetricRegistry metricRegistry;

    private static Logger log = LoggerFactory.getLogger( RecoveryLatePointLogger.class );

    private final Counter recoveryDelayed;

    private final Quota latePointsLogQuota = new Quota(100, 60);

    private final Histogram aggregatorRecoveryPointAge;

    public RecoveryLatePointLogger(MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;
        this.recoveryDelayed = metricRegistry.counter(
                MetricRegistry.name( "aggregator", "recoveryDelayed" ) );
        this.aggregatorRecoveryPointAge = metricRegistry.histogram(
                MetricRegistry.name("aggregator", "recoveryPointAgeHistogram"));
    }

    @Override
    public void logLatePoint(DataPoint m, long now, Reason r, String context) {
        // record point age only for skipped points to avoid overhead of updating histogram for every point
        long age = now - m.ts;
        aggregatorRecoveryPointAge.update(age);

        recoveryDelayed.inc();

        if( latePointsLogQuota.allow() )
        {
            log.warn( String.format(
                    "recovery data point skipped aggregation because it was received too late: age [%s], point: [%s]. reason: [%s] context: [%s]",
                    age, m, r, context) );
        }
    }
}
