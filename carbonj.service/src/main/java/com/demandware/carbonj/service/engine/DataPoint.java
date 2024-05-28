/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.joda.time.DateTime;

import com.demandware.carbonj.service.strings.StringsCache;

public class DataPoint
{
    public static final int INPUT_POINT_PRECISION = 60;

    public static final int UNKNOWN_ID = -1; // Not using 0 because Metric Index assigns 0 to non-leaf entries.

    // can be set to null to indicate that the point is dropped (no longer valid)
    public String name;

    public final double val;

    public final int ts;

    public long metricId = UNKNOWN_ID;

    public DataPoint( String name, double val, int ts )
    {
        this(name, val, ts, true);
    }

    public DataPoint(String name, double val, int ts, boolean queryCache)
    {
        this.name = queryCache ? StringsCache.get(name) : name;
        this.ts = ts;
        this.val = val;
    }

    public static int align2Min( DateTime dt )
    {
        return (int) ( dt.getMillis() / 1000 / 60 * 60 );
    }

    public void setMetricId( long id )
    {
        this.metricId = id;
    }

    public boolean hasMetricId()
    {
        return this.metricId != UNKNOWN_ID;
    }


    @Override
    public String toString()
    {
        return name + " " + strValue() + " " + ts;
    }

    public String strValue()
    {
        return strValue( val );
    }

    public static String strValue( double val )
    {
        // strip out irrelevant .00 at the end
        long r = (long) val;
        // long partial = Math.abs( (long) ( ( val - r ) * 100 ) ); // should be first two decimal digits
        long partial = Math.round( Math.abs( ( val - r ) * 100 ) ); // should be first two decimal digits
        String v = String.valueOf( r );
        if ( partial > 0 )
        {
            if ( partial < 10 )
            {
                v = v + ".0" + partial;
            }
            else
            {
                v = v + "." + ( 0 == partial % 10 ? partial / 10 : partial );
            }
        }
        return v;
    }

    public void drop()
    {
        this.name = null;
    }

    public boolean isValid()
    {
        return this.name != null && this.ts > 0;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
        result = prime * result + ts;
        long temp;
        temp = Double.doubleToLongBits( val );
        result = prime * result + (int) ( temp ^ ( temp >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        DataPoint other = (DataPoint) obj;
        if ( name == null )
        {
            if ( other.name != null )
            {
                return false;
            }
        }
        else if ( !name.equals( other.name ) )
        {
            return false;
        }
        if ( ts != other.ts )
        {
            return false;
        }
        return Double.doubleToLongBits(val) == Double.doubleToLongBits(other.val);
    }

    public int intervalValue(int precision)
    {
        return ts - ( ts % precision );
    }
}
