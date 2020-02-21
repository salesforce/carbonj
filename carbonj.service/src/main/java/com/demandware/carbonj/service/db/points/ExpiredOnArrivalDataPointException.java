/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.db.model.RetentionPolicy;

//TODO: consider removing
class ExpiredOnArrivalDataPointException
                extends RuntimeException
{
    private int ts;
    private double val;
    private RetentionPolicy policy;

    ExpiredOnArrivalDataPointException( int ts, double val, RetentionPolicy policy )
    {
        super("Point [ts=" + ts + ", va=" + val + "] is too old for highest precision retention policy [" + policy + "].");
        this.ts = ts;
        this.val = val;
        this.policy = policy;
    }

    //TODO: Rewrite unit tests to no require equals/hashCode impl.

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !( o instanceof ExpiredOnArrivalDataPointException ) )
            return false;

        ExpiredOnArrivalDataPointException that = (ExpiredOnArrivalDataPointException) o;

        if ( ts != that.ts )
            return false;
        if ( Double.compare( that.val, val ) != 0 )
            return false;
        return policy.equals( that.policy );

    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = ts;
        temp = Double.doubleToLongBits( val );
        result = 31 * result + (int) ( temp ^ ( temp >>> 32 ) );
        result = 31 * result + policy.hashCode();
        return result;
    }
}
