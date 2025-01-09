/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInterval {
    @Test
    public void testInterval() {
        Interval interval = new Interval(0, 60);
        assertTrue(interval.equals(interval));
        assertFalse(interval.equals(new Object()));
        assertFalse(interval.equals(new Interval(1, 1)));
        assertTrue(interval.equals(new Interval(0, 60)));
        assertTrue(interval.hashCode() > 0);
    }
}
