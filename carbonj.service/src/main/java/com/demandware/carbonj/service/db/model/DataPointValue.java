/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import com.demandware.carbonj.service.engine.DataPoint;

public class DataPointValue
{
    public final int ts;
    public final double val;

    public DataPointValue( int ts, double val)
    {
        this.ts = ts;
        this.val = val;
    }

    public String toString()
    {
        return ts + " " + DataPoint.strValue(val);
    }

}
