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
}
