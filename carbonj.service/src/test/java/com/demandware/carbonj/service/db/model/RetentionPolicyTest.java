/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RetentionPolicyTest
{
    @Test
    public void testParseLine()
    {
        List<RetentionPolicy> expected = Arrays.asList(
                        RetentionPolicy.getInstance( "60s:24h" ),
                        RetentionPolicy.getInstance( "5m:7d" ),
                        RetentionPolicy.getInstance( "30m:2y"));
        assertEquals(RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y" ), expected);
    }

    @Test
    public void testNegatives() {
        try {
            RetentionPolicy.getInstance("5t");
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Unsupported time unit suffix [t] in value [5t]", e.getMessage());
        }
        RetentionPolicy retentionPolicy = RetentionPolicy.getInstance("60s:1h");
        try {
            retentionPolicy.assertTimestampMatchesThisPolicyInterval(1);
            fail("Should have thrown an exception");
        } catch (RuntimeException e) {
            assertEquals("timestamp does not match any interval from this retention policy. ts: 1, retention policy interval for ts: 0", e.getMessage());
        }
        assertEquals(retentionPolicy, RetentionPolicy.higherPrecision(retentionPolicy, null));
        assertTrue(retentionPolicy.equals(RetentionPolicy.getInstance("60s:1h")));
        assertFalse(retentionPolicy.equals(new Object()));
        assertFalse(retentionPolicy.equals(RetentionPolicy.getInstance("60s:24h")));
        assertEquals("RetentionPolicy{name='60s:1h', precision=60, retention=3600}", retentionPolicy.toString());
        assertEquals(0, retentionPolicy.maxPoints(0, 0, 3601));
    }
}
