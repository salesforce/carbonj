/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import java.util.concurrent.atomic.AtomicInteger;

public class Quota
{
    private final int maxEvents;

    private final int resetInterval;

    private volatile long nextReset;

    private final AtomicInteger count = new AtomicInteger( 0 );

    public Quota( int maxEvents, int resetInterval )
    {
        this.maxEvents = maxEvents;
        this.resetInterval = resetInterval;
        this.nextReset = System.currentTimeMillis() + resetInterval * 1000;
    }

    public boolean allow()
    {
        long now = System.currentTimeMillis();
        if ( now > nextReset )
        {
            count.set( 0 );
            nextReset = now + resetInterval * 1000;
        }
        if ( count.incrementAndGet() < maxEvents )
        {
            return true;
        }
        else
        {
            return false;
        }
    }
}
