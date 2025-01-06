/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

public class MsgPackMetric
{
    public String path;
    public boolean isLeaf;

    public MsgPackMetric() {
    }

    public MsgPackMetric( Metric metric )
    {
        this.path = metric.name;
        this.isLeaf = metric.isLeaf();
    }
}
