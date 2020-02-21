/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.demandware.carbonj.service.engine.DataPoint;

import java.util.concurrent.atomic.AtomicInteger;

public class LoadBalancedDestination implements LineProtocolDestination {

    private LineProtocolDestination[] destinations;
    private AtomicInteger currentIndex = new AtomicInteger(0);

    public LoadBalancedDestination(LineProtocolDestination[] destinations) {
        super();
        this.destinations = destinations;
    }

    @Override
    public void closeQuietly() {
        for (LineProtocolDestination lineDestination: destinations) {
            lineDestination.closeQuietly();
        }
    }

    @Override
    public void accept(DataPoint dataPoint) {
        int noOfDestinations = destinations.length;
        if (noOfDestinations == 0) {
            return;
        }

        int index = currentIndex.getAndUpdate(n -> (n + 1 >= noOfDestinations) ? 0 : n + 1);
        destinations[index].accept(dataPoint);
    }

    public LineProtocolDestination[] getDestinations() {
        return destinations;
    }
}
