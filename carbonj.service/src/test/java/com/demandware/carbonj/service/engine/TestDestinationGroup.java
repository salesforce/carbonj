/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.destination.FileDestination;
import com.demandware.carbonj.service.engine.destination.KinesisDestination;
import com.demandware.carbonj.service.engine.destination.LineProtocolDestination;
import com.demandware.carbonj.service.engine.destination.LineProtocolDestinationSocket;
import com.demandware.carbonj.service.engine.destination.LineProtocolDestinationsService;
import com.demandware.carbonj.service.engine.destination.NullDestination;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertEquals(1, dest1.size());
        assertEquals(1, dest2.size());
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
        assertEquals(5, i);
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

    @Test
    public void test() throws Exception {
        FileUtils.writeLines(new File("/tmp/kinesis-test-stream-dest.conf"), List.of("kinesisBatchSize=1"));
        DestinationGroup destinationGroup = new DestinationGroup(metricRegistry, "relay", "",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "");
        assertNull(destinationGroup.getDestinations());
        destinationGroup = new DestinationGroup(metricRegistry, "relay", "null:foobar",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "");
        assertInstanceOf(NullDestination.class, destinationGroup.getDestinations()[0]);
        destinationGroup = new DestinationGroup(metricRegistry, "relay", "file:/tmp/destination",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "");
        assertInstanceOf(FileDestination.class, destinationGroup.getDestinations()[0]);
        destinationGroup = new DestinationGroup(metricRegistry, "relay", "kinesis:test-stream",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "");
        assertInstanceOf(KinesisDestination.class, destinationGroup.getDestinations()[0]);
        destinationGroup = new DestinationGroup(metricRegistry, "relay", "service:127.0.0.1:8080",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "");
        assertInstanceOf(LineProtocolDestinationsService.class, destinationGroup.getDestinations()[0]);
        destinationGroup = new DestinationGroup(metricRegistry, "relay", "127.0.0.1:8080",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "");
        assertInstanceOf(LineProtocolDestinationSocket.class, destinationGroup.getDestinations()[0]);
        assertTrue(destinationGroup.equals(destinationGroup));
        assertFalse(destinationGroup.equals(new Object()));
        assertFalse(destinationGroup.equals(new DestinationGroup(metricRegistry, "relay", "127.0.0.1:8088",
                1, 1, 60000, "/tmp", 1, Region.US_EAST_1.id(), false, "", "")));
        assertNotEquals(0, destinationGroup.hashCode());
        assertEquals("127.0.0.1:8080", destinationGroup.getDest());
        destinationGroup.close();
    }
}
