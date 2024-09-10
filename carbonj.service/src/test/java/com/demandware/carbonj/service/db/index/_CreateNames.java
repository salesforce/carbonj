/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class _CreateNames {
    File dbDirFile;
    MetricIndex index;
    NameUtils nameUtils;

    @BeforeEach
    public void setUp() {
        nameUtils = new NameUtils(InternalConfig.getRootEntryKey());
        dbDirFile = Files.createTempDir();
        index = IndexUtils.metricIndex( dbDirFile, false );
        index.open();
        ((MetricIndexImpl) index).parseDbProperties("estimate-num-keys", null);
    }

    @AfterEach
    public void tearDown() {
        if ( index != null ) {
            index.close();
        }

        if ( dbDirFile != null ) {
            dbDirFile.delete();
        }
    }

    @Test
    public void newNameIsAlwaysCreatedAsLeaf() {
        index.setStrictMode( true );

        // create metric
        Metric aMetric = index.createLeafMetric( "a" );
        assertTrue(aMetric.isLeaf());

        index.dumpStats();
        // name key: root, a plus an update to root
        // id key: 1:root, 2:a
        checkStats(3, 2);
    }


    @Test
    public void doNotAllowNewNodesForLeaf() {
        index.setStrictMode( true );
        
        // create metric
        Metric aMetric = index.createLeafMetric( "a.b.c" );
        assertTrue(aMetric.isLeaf());

        // 1. verify exception is thrown
        try {
            index.createLeafMetric( "a.b.c.d" );
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals("Cannot create metric with name [a.b.c.d] because [a.b.c] is already a leaf",
                e.getMessage());
        }
        // 2. verify we didn't leave any partial nodes behind
        assertNull(index.getMetric( "a.b.c.d" ));

        index.dumpStats();
        // name key: root, a.b.c, a.b, a plus an update to root
        // id key: 1:root, 2:a.b.c
        checkStats(5, 2);
    }

    @Test
    public void doNotAllowNewNodesForLeaf2() {
        index.setStrictMode( true );

        // create metric
        Metric aMetric = index.createLeafMetric( "a.b.c" );
        assertTrue(aMetric.isLeaf());

        // verify exception is thrown
        try {
            index.createLeafMetric( "a.b.c.d.e.f" );
            fail("Expected exception");
        } catch(RuntimeException e) {
            assertEquals("Cannot create metric with name [a.b.c.d.e.f] because [a.b.c] is already a leaf",
                e.getMessage());
        }

        // 2. verify we didn't leave any partial nodes behind
        assertNull(index.getMetric( "a.b.c.d" ));
        assertNull(index.getMetric( "a.b.c.d.e" ));
        assertNull(index.getMetric( "a.b.c.d.e.f" ));

        index.dumpStats();
        // name key: root, a.b.c, a.b, a plus an update to root
        // id key: 1:root, 2:a.b.c
        checkStats(5, 2);
    }

    private void checkStats(long expectedIndexNameCount, long expectedIndexIdCount) {
        assertEquals(1, ((MetricIndexImpl) index).getNameIndexStorePropertyMetricMap().size());
        assertTrue(((MetricIndexImpl) index).getNameIndexStorePropertyMetricMap().containsKey("estimate-num-keys"));
        assertEquals(expectedIndexNameCount, ((MetricIndexImpl) index).getNameIndexStorePropertyMetricMap().get("estimate-num-keys").getCount());
        assertEquals(1, ((MetricIndexImpl) index).getIdIndexStorePropertyMetricMap().size());
        assertTrue(((MetricIndexImpl) index).getIdIndexStorePropertyMetricMap().containsKey("estimate-num-keys"));
        assertEquals(expectedIndexIdCount, ((MetricIndexImpl) index).getIdIndexStorePropertyMetricMap().get("estimate-num-keys").getCount());
    }
}
