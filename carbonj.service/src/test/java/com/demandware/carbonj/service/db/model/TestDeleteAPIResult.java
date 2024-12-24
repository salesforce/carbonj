/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDeleteAPIResult {
    @Test
    public void test() {
        DeleteAPIResult deleteAPIResult = new DeleteAPIResult();
        deleteAPIResult.setResponseTtime(1);
        assertEquals(1, deleteAPIResult.getResponseTtime());
        deleteAPIResult.setSuccess(true);
        assertTrue(deleteAPIResult.getSuccess());
        deleteAPIResult.setError("error");
        assertEquals("error", deleteAPIResult.getError());
        assertTrue(deleteAPIResult.getMetricsList().isEmpty());
    }
}
