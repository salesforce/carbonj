/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class _RetentionPolicy
{
    @Test
    public void selectPolicy()
    {
        RetentionPolicy p = RetentionPolicy.getInstance( "60s:24h" );
        int from = 1459201593;
        int until=1459287993;
        int now=1459287993;
        assertTrue(p.includes(until, now));
        assertTrue(p.includes(from, now));

        assertFalse(p.includes(now - (int) TimeUnit.HOURS.toSeconds(25), now));
        assertTrue(p.includes(now - (int) TimeUnit.HOURS.toSeconds(23), now));
    }

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
    public void testDatabaseNameToPolicyNameConversion()
    {
        assertEquals(RetentionPolicy.getInstanceForDbName( "60s24h" ).name, "60s:24h");
        assertEquals(RetentionPolicy.getInstanceForDbName( "5m7d" ).name, "5m:7d");
        assertEquals(RetentionPolicy.getInstanceForDbName( "30m2y" ).name, "30m:2y");
    }
}
