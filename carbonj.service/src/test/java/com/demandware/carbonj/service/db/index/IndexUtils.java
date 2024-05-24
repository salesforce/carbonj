/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.StorageAggregationPolicySource;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;

import java.io.File;

/**
 * Test utils
 */
public class IndexUtils
{
    static MetricRegistry metricRegistry = new MetricRegistry();

    static IndexStore<String, NameRecord> nameIndexStore;

    private static IndexStore<String, NameRecord> metricNameIndexStore( File dbDir, boolean longId ) {
        return metricNameIndexStore(dbDir, longId, false);
    }

    private static IndexStore<String, NameRecord> metricNameIndexStore( File dbDir, boolean longId, boolean rocksdbReadonly )
    {
        return new IndexStoreRocksDB<>(metricRegistry, "index-name", dir( dbDir, "index-name" ), new NameRecordSerializer(longId), rocksdbReadonly, 1);
    }

    private static IndexStore<Long, IdRecord> metricIdIndexStore( File dbDir, boolean longId ) {
        return metricIdIndexStore(dbDir, longId, false);
    }

    private static IndexStore<Long, IdRecord> metricIdIndexStore( File dbDir, boolean longId, boolean rocksdbReadonly ) {
        return new IndexStoreRocksDB<>(metricRegistry, "index-id", dir( dbDir, "index-id" ), new IdRecordSerializer(longId), rocksdbReadonly, 1);
    }

    public static MetricIndex metricIndex( File dbDir, boolean longId )
    {
        nameIndexStore = metricNameIndexStore( dbDir, longId );
        return metricIndex( nameIndexStore, metricIdIndexStore( dbDir, longId ), databaseMetrics(), longId, "does-not-exist" );
    }

    public static MetricIndex metricIndexReadonly( File dbDir, boolean longId )
    {
        return metricIndex( metricNameIndexStore( dbDir, longId, true ), metricIdIndexStore( dbDir, longId, true ), databaseMetrics(), longId, "does-not-exist" );
    }

    public static MetricIndex metricIndex( File dbDir, boolean longId, String metricStoreConfig )
    {
        return metricIndex( metricNameIndexStore( dbDir, longId ), metricIdIndexStore( dbDir, longId ), databaseMetrics(), longId, metricStoreConfig );
    }

    private static MetricIndex metricIndex( IndexStore<String, NameRecord> nameIndex,
                                            IndexStore<Long, IdRecord> idIndex,
                                            DatabaseMetrics dbMetrics, boolean longId,
                                            String metricStoreConfig)
    {
        StorageAggregationRulesLoader rulesLoader = new StorageAggregationRulesLoader( new File("unknownFile") );
        StorageAggregationPolicySource policySource = new StorageAggregationPolicySource( rulesLoader );

        return new MetricIndexImpl(new MetricRegistry(), metricStoreConfig, nameIndex, idIndex, dbMetrics, 10000, 60,
                new NameUtils(),  policySource, 2000,120, false, longId, 10000, 60);
    }

    private static DatabaseMetrics databaseMetrics()
    {
        return new DatabaseMetrics(metricRegistry);
    }

    private static File dir( File dir, String dbName )
    {
        return new File( dir, dbName );
    }

    public static Metric findOrCreate( MetricIndex index, String name )
    {
        Metric m = index.getMetric( name );
        if ( m == null )
        {
            m = index.createLeafMetric( name );
        }
        return m;
    }

}
