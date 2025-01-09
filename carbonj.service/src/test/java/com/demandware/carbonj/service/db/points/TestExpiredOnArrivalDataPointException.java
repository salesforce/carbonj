/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.db.model.RetentionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExpiredOnArrivalDataPointException {
    @Test
    public void test() {
        ExpiredOnArrivalDataPointException exception = new ExpiredOnArrivalDataPointException(60, 123, RetentionPolicy.getInstance("60s:24h"));
        assertNotEquals(0, exception.hashCode());
        assertTrue(exception.equals(exception));
        assertFalse(exception.equals(new Object()));
        assertFalse(exception.equals(new ExpiredOnArrivalDataPointException(120, 123, RetentionPolicy.getInstance("60s:24h"))));
        assertFalse(exception.equals(new ExpiredOnArrivalDataPointException(60, 1234, RetentionPolicy.getInstance("60s:24h"))));
        assertTrue(exception.equals(new ExpiredOnArrivalDataPointException(60, 123, RetentionPolicy.getInstance("60s:24h"))));
    }
}
