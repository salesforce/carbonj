/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMsgPackMetric {
    @Test
    public void test() {
        Metric metric = new Metric("foo.bar", 1, null, List.of(RetentionPolicy.getInstance("60s:24h")), new ArrayList<>());
        MsgPackMetric msgPackMetric = new MsgPackMetric(metric);
        assertEquals(NullMetric.getInstance(), NullMetric.METRIC_NULL);
    }
}
