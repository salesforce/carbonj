/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class _RetentionPolicy
{
    @Before
    public void setUp()
    {
    }

    @Test
    public void selectPolicy()
    {
        RetentionPolicy p = RetentionPolicy.getInstance( "60s:24h" );
        int from = 1459201593;
        int until=1459287993;
        int now=1459287993;
        assertThat(p.includes(until, now), equalTo( true ));
        assertThat(p.includes(from, now), equalTo( true ));

        Assert.assertFalse(p.includes(now - (int) TimeUnit.HOURS.toSeconds(25), now));
        Assert.assertTrue(p.includes(now - (int) TimeUnit.HOURS.toSeconds(23), now));
    }

    @Test
    public void testParseLine()
    {

        List<RetentionPolicy> expected = Arrays.asList(
                        RetentionPolicy.getInstance( "60s:24h" ),
                        RetentionPolicy.getInstance( "5m:7d" ),
                        RetentionPolicy.getInstance( "30m:2y"));
        assertThat(RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y" ), equalTo(expected));
    }

    @Test
    public void testDatabaseNameToPolicyNameConversion()
    {
        assertThat(RetentionPolicy.getInstanceForDbName( "60s24h" ).name, equalTo( "60s:24h" ));
        assertThat(RetentionPolicy.getInstanceForDbName( "5m7d" ).name, equalTo( "5m:7d" ));
        assertThat(RetentionPolicy.getInstanceForDbName( "30m2y" ).name, equalTo( "30m:2y" ));
    }
}
