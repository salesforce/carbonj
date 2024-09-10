/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertEquals(expectedSlotTs, slotStrategy.getSlotTs(metricTs));
        assertEquals(expectedSlotStartTs, slotStrategy.getStartTs(metricTs));
        assertEquals(expectedSlotEndTs, slotStrategy.getEndTs(metricTs));
    }

    private void verify(SlotStrategy slotStrategy, int metricTs, int expectedSlotTs) {
        assertEquals(expectedSlotTs, slotStrategy.getSlotTs(metricTs));
    }
}
