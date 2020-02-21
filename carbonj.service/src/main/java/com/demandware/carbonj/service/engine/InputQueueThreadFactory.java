/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class InputQueueThreadFactory implements ThreadFactory
{
    private final AtomicInteger count = new AtomicInteger( 0 );

    private final String name;

    public InputQueueThreadFactory(String name)
    {
        this.name = name;
    }

    @Override
    public Thread newThread( Runnable r )
    {
        String threadName = name + count.incrementAndGet();
        Thread t = new Thread(r, threadName);
        t.setDaemon( true );
        return t;
    }
}
