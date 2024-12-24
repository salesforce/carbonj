/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.demandware.carbonj.service.db.util.time.TimeSource;
import com.demandware.carbonj.service.engine.AggregationMethod;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class _Metric
{
    int now;

    @BeforeEach
    public void setUp()
    {
        now = TimeSource.defaultTimeSource().getEpochSecond();
    }

    @Test
    public void shouldReturnMaxRetentionIntervalWithZerosForNonLeafMetric()
    {
        Metric m = new Metric( "a.b", 1, null, new ArrayList<>(  ), Arrays.asList("ch1", "ch2") );
        assertEquals(m.getMaxRetentionInterval(now), new Interval( 0, 0 ));
    }

    @Test
    public void shouldReturnMaxRetentionIntervalForLeafMetric()
    {
        List<RetentionPolicy> retentionPolicies = RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y" );
        Metric m = new Metric( "a.b", 1, null, retentionPolicies, null );
        Interval interval = m.getMaxRetentionInterval(now);

        int expected = 2 * 365 * 24 * 60 * 60; // 2years in seconds.
        assertEquals(interval.end - interval.start, expected);
    }

    @Test
    public void testNegatives() {
        File storageConfFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("storage-aggregation.conf")).getFile());
        AggregationPolicy aggregationPolicy = new AggregationPolicy(AggregationMethod.AVG, 0,
                new StorageAggregationPolicySource(new StorageAggregationRulesLoader(storageConfFile)));
        Metric m = new Metric( "a.b", 1, null, new ArrayList<>(), null );
        assertTrue(m.retentionPolicyAfter(null).isEmpty());
        assertEquals(0, m.getMaxRetention());
        m = new Metric( "a.b", 1, aggregationPolicy, null, null );
        assertTrue(m.getHighestPrecisionArchive().isEmpty());
        assertTrue(m.pickArchiveForQuery(0, 0, 0).isEmpty());
        assertFalse(m.getAggregationPolicy().configChanged());
        assertFalse(m.equals(new Object()));
        assertEquals("Metric{id=1, name='a.b'}", m.toString());
    }
}
