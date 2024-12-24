/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDataPointRecord {
    @Test
    public void test() {
        byte[] bytes = DataPointRecord.toKeyBytes(2, 60, true);
        assertEquals(2, DataPointRecord.toMetricId(bytes, true));
        bytes = DataPointRecord.toKeyBytes(3, 60, false);
        assertEquals(3, DataPointRecord.toMetricId(bytes, false));
    }
}
