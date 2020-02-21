/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

public interface SlotStrategy {

    /**
     * get slot timestamp for the given metric time stamp;
     * @param metricTs timestamp in seconds.
     * @return slot in seconds.
     */
    int getSlotTs(int metricTs);

    /**
     * For a given slot in seconds,  returns the starting time stamp of the slot. Any metric with its timestamp between
     * starting time stamp and ending time stamp of a slot,  gets aggregated into that slot.
     *
     * @param slotTs slot timestamp in seconds.
     * @return starting time stamp in seconds.
     */
    int getStartTs(int slotTs);

    /**
     * For a given slot in seconds,  returns the ending time stamp of the slot.  Any metric with its timestamp between
     * starting time stamp and ending time stamp of a slot,  gets aggregated into that slot.
     * @param slotTs slot timestamp in seconds.
     * @return ending time stamp in seconds.
     */
    int getEndTs(int slotTs);
}
