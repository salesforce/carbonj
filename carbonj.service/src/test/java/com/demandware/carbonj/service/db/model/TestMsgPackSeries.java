/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMsgPackSeries {
    @Test
    public void testSeries() {
        Series series = new Series("name", 0, 60, 60, List.of(1.0, 2.0));
        assertEquals("Series{name='name', start=0, end=60, step=60, values=[1.0, 2.0]}", series.toString());
        MsgPackSeries msgPackSeries = new MsgPackSeries(series);
        assertEquals("MsgPackSeries{start=0, end=60, step=60, name=name, pathExpression=name, values=[1.0, 2.0]}", msgPackSeries.toString());
    }
}
