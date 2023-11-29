/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

/**
 * Determines if time series data from a particular database should be cached or not.
 */
public class QueryCachePolicy
{
    final private boolean useTimeSeriesCacheFor60s24h;
    final private boolean useTimeSeriesCacheFor60s30d;
    final private boolean useTimeSeriesCacheFor5m7d;
    final private boolean useTimeSeriesCacheFor30m2y;

    public QueryCachePolicy(boolean useTimeSeriesCacheFor60s24h,
                            boolean useTimeSeriesCacheFor60s30d,
                            boolean useTimeSeriesCacheFor5m7d,
                            boolean useTimeSeriesCacheFor30m2y)
    {
        this.useTimeSeriesCacheFor60s24h = useTimeSeriesCacheFor60s24h;
        this.useTimeSeriesCacheFor60s30d = useTimeSeriesCacheFor60s30d;
        this.useTimeSeriesCacheFor5m7d = useTimeSeriesCacheFor5m7d;
        this.useTimeSeriesCacheFor30m2y = useTimeSeriesCacheFor30m2y;
    }

    public boolean useCache(RetentionPolicy p)
    {
        if( p == null )
        {
            return false;
        }

        if( p.is60s24h() )
        {
            return useTimeSeriesCacheFor60s24h;
        }

        if (p.is60s30d()) {
            return useTimeSeriesCacheFor60s30d;
        }

        if( p.is5m7d() )
        {
            return useTimeSeriesCacheFor5m7d;
        }

        if( p.is30m2y() )
        {
            return useTimeSeriesCacheFor30m2y;
        }

        return false;
    }

}
