/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.BaseTest;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.StorageAggregationPolicySource;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;

/**
 * Test utils
 */
public class IndexUtils
{
    static MetricRegistry metricRegistry = new MetricRegistry();
    private static IndexStore<String, NameRecord> metricNameIndexStore( File dbDir )
    {
        return new IndexStoreRocksDB<>( metricRegistry, "index-name", dir( dbDir, "index-name" ), new NameRecordSerializer() );
    }

    private static IndexStore<Integer, IdRecord> metricIdIndexStore( File dbDir )
    {
        return new IndexStoreRocksDB<>(metricRegistry, "index-id", dir( dbDir, "index-id" ), new IdRecordSerializer() );
    }

    public static MetricIndex metricIndex( File dbDir )
    {
        return metricIndex( metricNameIndexStore( dbDir ), metricIdIndexStore( dbDir ), databaseMetrics() );
    }

    private static MetricIndex metricIndex( IndexStore<String, NameRecord> nameIndex,
                                            IndexStore<Integer, IdRecord> idIndex, DatabaseMetrics dbMetrics )
    {
        StorageAggregationRulesLoader rulesLoader = new StorageAggregationRulesLoader( new File("unknownFile") );
        StorageAggregationPolicySource policySource = new StorageAggregationPolicySource( rulesLoader );

        return new MetricIndexImpl( metricRegistry,"doesnt-exist.conf", nameIndex, idIndex, dbMetrics, 10000, 60,
                new NameUtils(),  policySource, 2000,120, false);
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
