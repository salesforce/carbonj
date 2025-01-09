/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.Test;

import java.util.stream.DoubleStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAggregationMethod {
    @Test
    public void test() throws Exception {
        assertEquals(1.5, AggregationMethod.AVG.apply(DoubleStream.of(1.0, 2.0)));
        assertEquals(3.0, AggregationMethod.SUM.apply(DoubleStream.of(1.0, 2.0)));
        assertEquals(1.0, AggregationMethod.MIN.apply(DoubleStream.of(1.0, 2.0)));
        assertEquals(2.0, AggregationMethod.MAX.apply(DoubleStream.of(1.0, 2.0)));
        assertEquals(1.0, AggregationMethod.LAST.apply(DoubleStream.of(1.0, 2.0)));
    }
}
