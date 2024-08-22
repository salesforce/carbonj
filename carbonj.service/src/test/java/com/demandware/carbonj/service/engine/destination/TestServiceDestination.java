/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestServiceDestination {

    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Test
    public void testLoadBalancingAndClosure() {
        List<DataPoint> dest1List = new ArrayList<>(), dest2List = new ArrayList<>(), dest3List = new ArrayList<>();
        DestinationFetcher mockDestinationFetcher = mock(DestinationFetcher.class);
        when(mockDestinationFetcher.getCurrentDestinations()).thenReturn(
                new ListBackedDestination[] {new ListBackedDestination(dest1List),
                        new ListBackedDestination(dest2List), new ListBackedDestination(dest3List)});
        LineProtocolDestinationsService serviceDestination =
                new LineProtocolDestinationsService(metricRegistry, "relay", "relay-headless", -1,
                        mockDestinationFetcher);

        serviceDestination.accept(new DataPoint("metrics1", 1, 1));
        verify(dest1List, dest2List, dest3List, 1, 0, 0);

        serviceDestination.accept(new DataPoint("metrics2", 1, 2));
        verify(dest1List, dest2List, dest3List, 1, 1, 0);

        serviceDestination.accept(new DataPoint("metrics3", 1, 3));
        verify(dest1List, dest2List, dest3List, 1, 1, 1);

        serviceDestination.accept(new DataPoint("metrics4", 1, 3));
        verify(dest1List, dest2List, dest3List, 2, 1, 1);

        serviceDestination.accept(new DataPoint("metrics5", 1, 3));
        verify(dest1List, dest2List, dest3List, 2, 2, 1);

        serviceDestination.closeQuietly();
        verify(dest1List, dest2List, dest3List, 0, 0, 0);
    }

    @Test
    public void testDestinationChange() {
        List<DataPoint> dest1List = new ArrayList<>(), dest2List = new ArrayList<>(), dest3List = new ArrayList<>();

        List<DataPoint> dest4List = new ArrayList<>(), dest5List = new ArrayList<>();

        DestinationFetcher mockDestinationFetcher = mock(DestinationFetcher.class);
        when(mockDestinationFetcher.getCurrentDestinations())
                .thenReturn(new ListBackedDestination[] {new ListBackedDestination(dest1List),
                        new ListBackedDestination(dest2List), new ListBackedDestination(dest3List)})
                .thenReturn(new ListBackedDestination[] {new ListBackedDestination(dest4List),
                        new ListBackedDestination(dest5List)});
        LineProtocolDestinationsService serviceDestination =
                new LineProtocolDestinationsService(metricRegistry, "relay", "relay-headless", -1,
                        mockDestinationFetcher);

        serviceDestination.accept(new DataPoint("metrics1", 1, 1));
        verify(dest1List, dest2List, dest3List, 1, 0, 0);

        serviceDestination.accept(new DataPoint("metrics2", 1, 2));
        verify(dest1List, dest2List, dest3List, 1, 1, 0);

        serviceDestination.accept(new DataPoint("metrics3", 1, 3));
        verify(dest1List, dest2List, dest3List, 1, 1, 1);

        new LineProtocolDestinationsService.RefreshDestinationsTask(serviceDestination, mockDestinationFetcher).run();
        verify(dest1List, dest2List, dest3List, 0, 0, 0);

        serviceDestination.accept(new DataPoint("metrics4", 1, 4));
        verify(dest4List, dest5List, dest3List, 1, 0, 0);

        serviceDestination.accept(new DataPoint("metrics5", 1, 5));
        verify(dest4List, dest5List, dest3List, 1, 1, 0);

        serviceDestination.accept(new DataPoint("metrics6", 1, 6));
        verify(dest4List, dest5List, dest3List, 2, 1, 0);

        serviceDestination.closeQuietly();
        verify(dest4List, dest5List, dest3List, 0, 0, 0);
    }

    private void verify(List<DataPoint> list1, List<DataPoint> list2, List<DataPoint> list3, int length1, int length2,
                        int length3) {
        assertEquals(length1, list1.size());
        assertEquals(length2, list2.size());
        assertEquals(length3, list3.size());
    }

    private static class ListBackedDestination implements  LineProtocolDestination {

        private final List<DataPoint> dataPoints;

        private ListBackedDestination(List<DataPoint> dataPoints) {
            this.dataPoints = dataPoints;
        }

        @Override
        public void closeQuietly() {
            dataPoints.clear();
        }

        @Override
        public void accept(DataPoint dataPoint) {
            dataPoints.add(dataPoint);
        }
    }
}
