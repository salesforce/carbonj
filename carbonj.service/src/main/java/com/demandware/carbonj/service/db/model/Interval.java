/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

public class Interval
{
    public final int start;
    public final int end;

    Interval(int start, int end)
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !( o instanceof Interval ) )
            return false;

        Interval interval = (Interval) o;

        if ( start != interval.start )
            return false;
        return end == interval.end;

    }

    @Override
    public int hashCode()
    {
        int result = start;
        result = 31 * result + end;
        return result;
    }
}
