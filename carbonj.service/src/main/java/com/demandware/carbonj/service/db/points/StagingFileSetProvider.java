/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class StagingFileSetProvider
{
    private final int cacheSize = 300;

    private final LoadingCache<String, StagingFileSet> cache = CacheBuilder.newBuilder()
                .maximumSize( cacheSize )
                .expireAfterWrite( 30,TimeUnit.MINUTES )
                .build(new CacheLoader<>() {
                    @SuppressWarnings("NullableProblems")
                    @Override
                    public StagingFileSet load(String key) {
                        return new StagingFileSet(key);
                    }
                } );

    StagingFileSet get(String dbName, int from, int group)
    {
        try
        {
            return cache.get(StagingFileSet.getId(dbName, from, group));
        }
        catch(ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
