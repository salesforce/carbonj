/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestDefaultSlotStrategy {

    @Test
    public void testSlotStrategy() {
        SlotStrategy slotStrategy = new DefaultSlotStrategy();
        verify(slotStrategy, 0, 0, 0, 59);
        verify(slotStrategy, 30, 0);
        verify(slotStrategy, 59, 0);
        verify(slotStrategy, 60, 60, 60, 119);
        verify(slotStrategy, 90, 60);
        verify(slotStrategy, 110, 60);
        verify(slotStrategy, 120, 120, 120, 179);
    }

    private void verify(SlotStrategy slotStrategy, int metricTs, int expectedSlotTs,  int expectedSlotStartTs, int expectedSlotEndTs) {
        Assert.assertEquals(expectedSlotTs, slotStrategy.getSlotTs(metricTs));
        Assert.assertEquals(expectedSlotStartTs, slotStrategy.getStartTs(metricTs));
        Assert.assertEquals(expectedSlotEndTs, slotStrategy.getEndTs(metricTs));
    }

    private void verify(SlotStrategy slotStrategy, int metricTs, int expectedSlotTs) {
        Assert.assertEquals(expectedSlotTs, slotStrategy.getSlotTs(metricTs));
    }
}
