/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

public class DefaultSlotStrategy implements SlotStrategy {

    @Override
    public int getSlotTs(int metricTs) {
        return (metricTs / 60) * 60;
    }

    @Override
    public int getStartTs(int slotTs) {
        return slotTs;
    }

    @Override
    public int getEndTs(int slotTs) {
        return slotTs + 59;
    }
}
