/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.db.util.time.TimeSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class _Metric
{
    int now;

    @Before
    public void setUp()
    {
        now = TimeSource.defaultTimeSource().getEpochSecond();
    }

    @Test
    public void shouldReturnMaxRetentionIntervalWithZerosForNonLeafMetric()
    {
        Metric m = new Metric( "a.b", 1, null, new ArrayList<>(  ), Arrays.asList("ch1", "ch2") );
        assertThat(m.getMaxRetentionInterval(now), equalTo(new Interval( 0, 0 )));
    }

    @Test
    public void shouldReturnMaxRetentionIntervalForLeafMetric()
    {
        List<RetentionPolicy> retentionPolicies = RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y" );
        Metric m = new Metric( "a.b", 1, null, retentionPolicies, null );
        Interval interval = m.getMaxRetentionInterval(now);

        int expected = 2 * 365 * 24 * 60 * 60; // 2years in seconds.
        assertThat(interval.end - interval.start, equalTo(expected));

    }
}
