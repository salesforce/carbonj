/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.demandware.carbonj.service.accumulator.DefaultSlotStrategy;
import com.demandware.carbonj.service.accumulator.SlotStrategy;
import com.demandware.carbonj.service.db.util.time.TimeSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import java.util.Date;

@RunWith(JUnit4.class)
public class TestSlotStrategy {

    @Test
    public void testBasic() {
        SlotStrategy slotStrategy = new DefaultSlotStrategy();
        int now = TimeSource.defaultTimeSource().getEpochSecond();
        int slotTs = slotStrategy.getSlotTs(now);

        int startTs = slotStrategy.getStartTs(slotTs);
        int endTs = slotStrategy.getEndTs(slotTs);

        System.out.println(new Date(startTs * 1000L));
        System.out.println(new Date(endTs * 1000L));
    }
}
