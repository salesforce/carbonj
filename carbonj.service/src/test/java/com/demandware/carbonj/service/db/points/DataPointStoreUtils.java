/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.QueryCachePolicy;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataPointStoreUtils {
    public static DataPointStore createDataPointStore(MetricRegistry metricRegistry, File dbDirFile, boolean longId,
                                                      MetricIndex metricIndex) {
        File stagingDir = new File(dbDirFile, "staging");
        assertTrue(stagingDir.mkdirs());
        StagingFiles sFiles = new StagingFiles(metricRegistry, stagingDir, new SystemSort(), metricIndex);
        DataPointArchiveFactory pointArchiveFactory = new DataPointArchiveFactory(metricRegistry, dbDirFile, new RocksDBConfig(), longId);
        DataPointStagingStore stagingStore = new DataPointStagingStore( metricRegistry, sFiles, 1000, 1, 1, 1, 100, 30, 3);
        QueryCachePolicy qcp = new QueryCachePolicy( true, true, true, true );
        return new DataPointStoreImpl(metricRegistry, pointArchiveFactory, new DatabaseMetrics(metricRegistry),
                stagingStore, true, 100, 10, qcp, name -> true );
    }
}
