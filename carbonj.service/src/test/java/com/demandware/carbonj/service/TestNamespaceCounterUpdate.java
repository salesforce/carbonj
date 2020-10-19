/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
//package com.demandware.carbonj.service;
//
//import com.demandware.carbonj.service.accumulator.*;
//import com.demandware.carbonj.service.db.util.time.TimeSource;
//import com.demandware.carbonj.service.engine.*;
//import com.demandware.carbonj.service.ns.NamespaceCounter;
//import org.junit.Assert;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.JUnit4;
//
//import java.util.Collections;
//
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//@RunWith(JUnit4.class)
//public class TestNamespaceCounterUpdate {
//
//    @Test
//    public void testNamespaceCounterUpdate() {
//        MetricAggregationPolicyProvider mock = getMetricAggregationPolicyProvider();
//        NamespaceCounter namespaceCounter = new NamespaceCounter(30);
//        AccumulatorImpl accumulator = new AccumulatorImpl(mock, 1000,  30,
//                new DefaultSlotStrategy(), namespaceCounter);
//        MetricList blackList = getBlackList();
//        DataPoint dataPoint = new DataPoint("metric1", 1, TimeSource.defaultTimeSource().getEpochSecond());
//        PointProcessorTask task = new PointProcessorTask(Collections.singletonList(dataPoint), blackList, null,
//                accumulator, true, null, this::accept, mock(Relay.class), namespaceCounter);
//        task.run();
//
//        Assert.assertTrue(namespaceCounter.exists("metric1"));
//        Assert.assertTrue(namespaceCounter.exists("sum"));
//    }
//
//    @SuppressWarnings("unused")
//    private void accept(DataPoints dataPoints) {}
//
//    private MetricAggregationPolicyProvider getMetricAggregationPolicyProvider() {
//        MetricAggregate sum = new MetricAggregate("sum.aggr", MetricAggregationMethod.SUM, false);
//        MetricAggregationPolicy map = new MetricAggregationPolicy(1, Collections.singletonList(sum));
//        MetricAggregationPolicyProvider mock = mock(MetricAggregationPolicyProvider.class);
//        when(mock.metricAggregationPolicyFor(any(String.class))).thenReturn(map);
//        return mock;
//    }
//
//    private MetricList getBlackList() {
//        MetricList blackList = mock(MetricList.class);
//        when(blackList.match(any(String.class))).thenReturn(false);
//        return blackList;
//    }
//}
