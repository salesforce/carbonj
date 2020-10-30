/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

import com.demandware.carbonj.service.db.model.RetentionPolicy;

class DataPointArchiveFactory
{
    final private ConcurrentHashMap<String, DataPointArchive> archiveStores = new ConcurrentHashMap<>(  );

    private MetricRegistry metricRegistry;

    private RocksDBConfig rocksDBConfig;

    private File dataDir;

    DataPointArchiveFactory(MetricRegistry metricRegistry, File dataDir, RocksDBConfig rocksDBConfig)
    {
        this.metricRegistry = metricRegistry;
        this.dataDir = Preconditions.checkNotNull( dataDir );
        this.rocksDBConfig = Preconditions.checkNotNull( rocksDBConfig );
    }

    DataPointArchive get( String dbName)
    {
        RetentionPolicy rp = RetentionPolicy.getInstanceForDbName( dbName );
        return get(rp);
    }

    DataPointArchive get( RetentionPolicy policy)
    {
        return archiveStores.computeIfAbsent( policy.dbName, key -> open(key, policy) );
    }

    private DataPointArchive open(String dbName, RetentionPolicy policy)
    {
        File dbDir = dbDir( dbName );
        DataPointArchive db = new DataPointArchiveRocksDB(metricRegistry, dbName, policy, dbDir, rocksDBConfig );
        db.open();
        return db;
    }

    public void dumpStats()
    {
        archiveStores.forEachValue( 5, v -> v.dumpStats());
    }

    public void close()
    {
        archiveStores.forEachValue(5, v -> v.close());
    }

    private File dbDir(String dbName)
    {
        return new File(dataDir, dbName);
    }
}
