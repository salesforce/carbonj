/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import org.junit.Assert;
import org.junit.Test;

public class _QueryCachePolicy
{
    RetentionPolicy _60s24h = RetentionPolicy.getInstance( "60s:24h" );
    RetentionPolicy _60s30d = RetentionPolicy.getInstance( "60s:30d" );
    RetentionPolicy _5m7d = RetentionPolicy.getInstance( "5m:7d" );
    RetentionPolicy _30m2y = RetentionPolicy.getInstance( "30m:2y" );

    @Test
    public void cacheEnabledFor60s24hOnly()
    {

        QueryCachePolicy qcp = new QueryCachePolicy( true, false, false, false );
        Assert.assertTrue(qcp.useCache(_60s24h));
        Assert.assertFalse(qcp.useCache( _60s30d ));
        Assert.assertFalse(qcp.useCache( _5m7d ));
        Assert.assertFalse(qcp.useCache( _30m2y ));
    }

    @Test
    public void cacheDisabledForAll()
    {
        QueryCachePolicy qcp = new QueryCachePolicy( false, false, false, false );
        Assert.assertFalse(qcp.useCache( _60s24h ));
        Assert.assertFalse(qcp.useCache( _60s30d ));
        Assert.assertFalse(qcp.useCache( _5m7d ));
        Assert.assertFalse(qcp.useCache( _30m2y ));
    }

    @Test
    public void cacheDisabledForNull()
    {
        QueryCachePolicy qcp = new QueryCachePolicy( true, true, true, true );
        Assert.assertFalse(qcp.useCache( null ));
    }

    @Test
    public void cacheDisabledForUnknown()
    {
        RetentionPolicy _120s2d = RetentionPolicy.getInstance( "120s:2d" );
        QueryCachePolicy qcp = new QueryCachePolicy( true, true, true, true );
        Assert.assertFalse(qcp.useCache( _120s2d ));
    }


    @Test
    public void cacheEnabledForAll()
    {
        QueryCachePolicy qcp = new QueryCachePolicy( true, true, true, true );
        Assert.assertTrue(qcp.useCache( _60s24h ));
        Assert.assertTrue(qcp.useCache( _60s30d ));
        Assert.assertTrue(qcp.useCache( _5m7d ));
        Assert.assertTrue(qcp.useCache( _30m2y ));
    }

    @Test
    public void cacheDisabledFor30m2yOnly()
    {
        QueryCachePolicy qcp = new QueryCachePolicy( true, true, true, false );
        Assert.assertTrue(qcp.useCache( _60s24h ));
        Assert.assertTrue(qcp.useCache( _60s30d ));
        Assert.assertTrue(qcp.useCache( _5m7d ));
        Assert.assertFalse(qcp.useCache( _30m2y ));
    }
}
