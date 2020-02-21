/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.base.Throwables;

public class DrainUtils
{
    public static void drain( ExecutorService ex )
    {
        if ( ex instanceof ThreadPoolExecutor )
        {
            ThreadPoolExecutor tp = (ThreadPoolExecutor) ex;
            while ( tp.getActiveCount() + tp.getQueue().size() > 0 )
            {
                try
                {
                    Thread.sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    throw Throwables.propagate( e );
                }
            }
        }
    }

    public static void drain( BlockingQueue<?> queue )
    {
        while ( queue.size() > 0 )
        {
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                throw Throwables.propagate( e );
            }
        }
    }
}
