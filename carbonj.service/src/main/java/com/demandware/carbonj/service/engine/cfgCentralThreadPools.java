/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@EnableScheduling
public class cfgCentralThreadPools
    implements SchedulingConfigurer
{
    @Value( "${centralThreadpoolExecutorSize:5}" )
    private int centralThreadpoolExecutorSize;

    @Override
    public void configureTasks( ScheduledTaskRegistrar taskRegistrar )
    {
        taskRegistrar.setScheduler( centralExecutor() );
    }

    @Bean( destroyMethod = "shutdown" )
    public ScheduledExecutorService centralExecutor()
    {
        ScheduledThreadPoolExecutor pool = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool( centralThreadpoolExecutorSize );
        pool.setKeepAliveTime( 60, TimeUnit.SECONDS );
        return pool;
    }
}
