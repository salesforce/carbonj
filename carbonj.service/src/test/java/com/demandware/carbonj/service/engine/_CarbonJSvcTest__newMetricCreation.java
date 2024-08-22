/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class _CarbonJSvcTest__newMetricCreation
                extends AbstractCarbonJ_StoreTest
{
    @Test
    public void newMetricCreation()
    {
        assertFalse( cjClient.listMetrics( "*" ).contains( "test" ) );

        cjClient.send( "test", 1.0f, new DateTime() );
        drain();

        assertEquals(List.of("test"), cjClient.listMetrics( "test" ) );
    }
}
