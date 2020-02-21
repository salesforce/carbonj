/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class _CarbonJSvcTest__newMetricCreation
                extends AbstractCarbonJ_StoreTest
{
    @Test public void newMetricCreation()
    {
        Assert.assertFalse( cjClient.listMetrics( "*" ).contains( "test" ) );

        cjClient.send( "test", 1.0f, new DateTime() );
        drain();

        _CarbonJSvcTest.assertEquals( Arrays.asList( "test" ), cjClient.listMetrics( "test" ) );
    }
}
