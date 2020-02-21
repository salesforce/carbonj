/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestLoadBalancedDestination {

    @Test
    public void testEmptyDestination() {
        LoadBalancedDestination loadBalancedDestination = new LoadBalancedDestination(new LineProtocolDestination[0]);
        loadBalancedDestination.accept(new DataPoint("metric1", 1, (int) System.currentTimeMillis()/1000));
    }
}
