/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class StagingFileSetProvider
{
    private int cacheSize = 20; // 3 archive types, usually there will be only one (current) interval per archive.
    private LoadingCache<String, StagingFileSet> cache = CacheBuilder.newBuilder()
                .maximumSize( cacheSize )
                .expireAfterWrite( 30,TimeUnit.MINUTES )
                .build( new CacheLoader<String, StagingFileSet>()
                    {
                        @Override
                        public StagingFileSet load( String key )
                            throws Exception
                        {
                            return new StagingFileSet( key );
                        }
                    } );


    StagingFileSet get(String dbName, int from)
    {
        try
        {
            return cache.get( toId( dbName, from ) );
        }
        catch(ExecutionException e)
        {
            throw Throwables.propagate( e );
        }
    }

    //TODO: refactor - id formatting logic is in StagingFilesSet and here now.
    private String toId(String dbName, int from)
    {
        return dbName + StagingFileSet.PARTS_DELIMITER + from;
    }
}
