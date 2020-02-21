/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.destination.LineProtocolDestination;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class TestDestinationGroup
                extends AbstractCarbonJBaseTest
{
    MetricRegistry metricRegistry = new MetricRegistry();

    @Test
    public void testLoadBalancing() {
        List<DataPoint> dest1 = new ArrayList<>();
        List<DataPoint> dest2 = new ArrayList<>();

        DestinationGroup destinationGroup = new DestinationGroup(metricRegistry, new LineProtocolDestination[]{
                new MemoryDestination(dest1), new MemoryDestination(dest2)
        });

        destinationGroup.accept(new DataPoint("metrics1", 2, 1));
        destinationGroup.accept(new DataPoint("metrics2", 2, 1));

        Assert.assertEquals(1, dest1.size());
        Assert.assertEquals(1, dest2.size());
    }

    @Test
    public void testIsolation() {
        LineProtocolDestination errorThrowingDestination = new LineProtocolDestination() {

            @Override
            public void closeQuietly() {
            }

            @Override
            public void accept(DataPoint dataPoint) {
                throw new RuntimeException();
            }
        };

        DestinationGroup destinationGroup = new DestinationGroup(metricRegistry, new LineProtocolDestination[]{errorThrowingDestination});

        int i = 0;
        while (i < 5) {
            destinationGroup.accept(new DataPoint("metric", 1, i++));
        }

        Assert.assertEquals(5, i);
    }

    private static class MemoryDestination implements LineProtocolDestination {

        List<DataPoint> dataPoints;

        MemoryDestination(List<DataPoint> dataPoints) {
            this.dataPoints = dataPoints;
        }

        @Override
        public void closeQuietly() {
        }

        @Override
        public void accept(DataPoint dataPoint) {
            dataPoints.add(dataPoint);
        }
    }
}
