/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

/**
 * Indicates that number of metrics found exceeds specified threshold value.
 */
public class TooManyMetricsFoundException extends RuntimeException {

    private int limit;

    public TooManyMetricsFoundException(int limit)
    {
        super();
        this.limit = limit;
    }

    public TooManyMetricsFoundException(int limit, String msg)
    {
        super(msg);
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }
}
