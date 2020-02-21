/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.util.StatsAware;

public class BlockingPolicy implements RejectedExecutionHandler
{
    private static final Logger log = LoggerFactory.getLogger( BlockingPolicy.class );

    private final Timer blocks;
    private final StatsAware stats;
    private final String name;
    private final boolean useLog;

    public BlockingPolicy(  Timer blockingTimer )
    {
        this(null, null, blockingTimer, false );
    }

    public BlockingPolicy( String name, StatsAware stats, Timer blockingTimer, boolean useLog )
    {
        this.blocks = blockingTimer;
        this.stats = stats;
        this.name = name;
        this.useLog = useLog;
    }

    @Override
    public void rejectedExecution( Runnable r, ThreadPoolExecutor e ) {

        if (log.isDebugEnabled()) {
            log.debug(String.format("%s thread pool is full (%s) - blocking.", name, e.getMaximumPoolSize()));
        }

        if (useLog)
        {
            log.info(String.format("%s thread pool is full (%s) - blocking.", name, e.getMaximumPoolSize()));
        }

        if( stats != null )
        {
            stats.refreshStats(); // good time to refresh stats
        }

        try(Timer.Context c = blocks.time())
        {
            e.getQueue().put( r );
        }
        catch(InterruptedException t)
        {
            throw new RejectedExecutionException( t );
        }
    }
}
