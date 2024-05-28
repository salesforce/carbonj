/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.demandware.carbonj.service.db.util.FileUtils;
import com.google.common.cache.LoadingCache;
import org.junit.Test;

import java.io.File;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.demandware.carbonj.service.db.model.NullMetric.METRIC_NULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexNameSyncTest extends BaseIndexTest {
    @Test
    public void testCacheRefresh() throws Exception {
        index.createLeafMetric("a.b.c");
        Metric metric = index.getMetric("a.b.c");
        assertEquals("a.b.c", metric.name);
        MetricIndexImpl metricIndex = (MetricIndexImpl)index;
        LoadingCache<String, Metric> metricCache = metricIndex.getMetricCache();
        Map<String, Metric> metricCacheMap = metricCache.asMap();
        assertTrue(metricCacheMap.containsKey("root"));
        assertNotEquals(metricCacheMap.get("root"), METRIC_NULL);
        assertTrue(metricCacheMap.containsKey("a"));
        assertNotEquals(metricCacheMap.get("a"), METRIC_NULL);
        assertTrue(metricCacheMap.containsKey("a.b"));
        assertNotEquals(metricCacheMap.get("a.b"), METRIC_NULL);
        assertTrue(metricCacheMap.containsKey("a.b.c"));
        assertNotEquals(metricCacheMap.get("a.b.c"), METRIC_NULL);

        // Run SyncSecondaryDbTask to dump metric names from queue into a file
        MetricIndexImpl.SyncSecondaryDbTask syncSecondaryDbTask = metricIndex.new SyncSecondaryDbTask();
        syncSecondaryDbTask.run();

        File syncDir = FileUtils.getSyncDirFromDbDir(IndexUtils.nameIndexStore.getDbDir());
        File[] syncFiles = syncDir.listFiles((dir, name) -> name.startsWith("sync-"));
        assertTrue(syncFiles != null && syncFiles.length == 1);

        // This simulates the scenario that index-name DB updated from write JVM, but cache in read JVM has not been updated
        metricCache.invalidate("a");
        metricCache.invalidate("a.b");
        metricCache.invalidate("a.b.c");
        metricCacheMap = metricCache.asMap();
        assertFalse(metricCacheMap.containsKey("a"));
        assertFalse(metricCacheMap.containsKey("a.b"));
        assertFalse(metricCacheMap.containsKey("a.b.c"));

        // Run SyncNameIndexCacheTask to simulate name index sync on read JVM from files generated from write JVM
        MetricIndexImpl.SyncNameIndexCacheTask syncNameIndexCacheTask = metricIndex.new SyncNameIndexCacheTask();
        syncNameIndexCacheTask.run();
        metricCacheMap = metricCache.asMap();
        assertTrue(metricCacheMap.containsKey("root"));
        assertNotEquals(metricCacheMap.get("root"), METRIC_NULL);
        assertTrue(metricCacheMap.get("root").children().contains("a"));
        assertTrue(metricCacheMap.containsKey("a"));
        assertNotEquals(metricCacheMap.get("a"), METRIC_NULL);
        assertTrue(metricCacheMap.get("a").children().contains("b"));
        assertTrue(metricCacheMap.containsKey("a.b"));
        assertNotEquals(metricCacheMap.get("a.b"), METRIC_NULL);
        assertTrue(metricCacheMap.get("a.b").children().contains("c"));
        assertTrue(metricCacheMap.containsKey("a.b.c"));
        assertNotEquals(metricCacheMap.get("a.b.c"), METRIC_NULL);
        assertTrue(metricCacheMap.get("a.b.c").children().isEmpty());

        // Simulate index-name DB updated but metric cached has not been updated on read JVM
        NameRecord nameRecord = new NameRecord("a.b.d", 100, true);
        nameRecord.setChildren(new ArrayList<>());
        nameRecord.setRetentionPolicies(new ArrayList<>() {{
            this.add(RetentionPolicy._60s24h());
        }});
        IndexUtils.nameIndexStore.dbPut(nameRecord);
        nameRecord = IndexUtils.nameIndexStore.dbGet("a.b.d");
        assertNotNull(nameRecord);
        assertEquals("a.b.d", nameRecord.getKey());

        nameRecord = IndexUtils.nameIndexStore.dbGet("a.b");
        nameRecord.addChildKeyIfMissing("a.b.d");
        IndexUtils.nameIndexStore.dbPut(nameRecord);

        // Simulate index-name dump from write JVM
        Set<String> metrics = new HashSet<>(Arrays.asList( "a.b", "a.b.d"));
        FileUtils.dumpSetToFile(metrics, new File(syncDir, "sync-" + Clock.systemUTC().millis()));

        // Simulate index-name sync on read JVM
        syncNameIndexCacheTask.run();
        metricCacheMap = metricCache.asMap();
        assertTrue(metricCacheMap.containsKey("a.b"));
        assertNotEquals(metricCacheMap.get("a.b"), METRIC_NULL);
        assertTrue(metricCacheMap.get("a.b").children().contains("d"));
        assertTrue(metricCacheMap.containsKey("a.b.d"));
        assertNotEquals(metricCacheMap.get("a.b.d"), METRIC_NULL);
        assertTrue(metricCacheMap.get("a.b.d").children().isEmpty());

        // Simulate root level metric add
        nameRecord = new NameRecord("x.y", 200, true);
        nameRecord.setChildren(new ArrayList<>());
        nameRecord.setRetentionPolicies(new ArrayList<>() {{
            this.add(RetentionPolicy._60s24h());
        }});
        IndexUtils.nameIndexStore.dbPut(nameRecord);
        nameRecord = IndexUtils.nameIndexStore.dbGet("x.y");
        assertNotNull(nameRecord);
        assertEquals("x.y", nameRecord.getKey());

        nameRecord = new NameRecord("x", 201, false);
        nameRecord.addChildKeyIfMissing("x.y");
        IndexUtils.nameIndexStore.dbPut(nameRecord);

        nameRecord = IndexUtils.nameIndexStore.dbGet("root");
        nameRecord.addChildKeyIfMissing("x");
        IndexUtils.nameIndexStore.dbPut(nameRecord);

        // Simulate index-name dump from write JVM
        metrics = new HashSet<>(Arrays.asList( "root", "x", "x.y"));
        FileUtils.dumpSetToFile(metrics, new File(syncDir, "sync-" + Clock.systemUTC().millis()));

        // Simulate index-name sync on read JVM
        syncNameIndexCacheTask.run();
        metricCacheMap = metricCache.asMap();
        assertTrue(metricCacheMap.containsKey("root"));
        assertNotEquals(metricCacheMap.get("root"), METRIC_NULL);
        assertTrue(metricCacheMap.get("root").children().contains("x"));
        assertTrue(metricCacheMap.containsKey("x"));
        assertNotEquals(metricCacheMap.get("x"), METRIC_NULL);
        assertTrue(metricCacheMap.get("x").children().contains("y"));
        assertTrue(metricCacheMap.containsKey("x.y"));
        assertNotEquals(metricCacheMap.get("x.y"), METRIC_NULL);
        assertTrue(metricCacheMap.get("x.y").children().isEmpty());
    }
}
