/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.db.index.IndexUtils;
import com.demandware.carbonj.service.db.model.IntervalValues;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestSortedStagingFile {
    @Test
    public void test() throws Exception {
        MetricIndex metricIndex = IndexUtils.metricIndex(new File("/tmp/testdb"), true);
        metricIndex.open();
        Metric metric1 = metricIndex.createLeafMetric("pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io");
        assertEquals(2, metric1.id);
        File staging = new File("/tmp/5m7d-1734989700-9.1.s");
        List<String> lines = List.of(
                "11987976699 2 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io",
                "11987976699 3 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io",
                "11987976699 1 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io",
                "11987984189 5239330 POD276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.a2su9qja883gs.1965276335.sqlmonitor.elapsed_time",
                "11987984189 5239330 POD276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.a2su9qja883gs.1965276335.sqlmonitor.elapsed_time");
        FileUtils.writeLines(staging, lines);
        SortedStagingFile sortedStagingFile = new SortedStagingFile(staging, metricIndex);
        sortedStagingFile.open();
        assertEquals("5m7d", sortedStagingFile.dbName());
        assertFalse(sortedStagingFile.isClosed());

        IntervalValues intervalValues = sortedStagingFile.loadNeighbours().get();
        assertEquals("5m7d", intervalValues.dbName);
        assertEquals(1734989700, intervalValues.intervalStart);
        assertEquals(metric1, intervalValues.metric);
        assertEquals(3, intervalValues.values.size());
        assertEquals(2, intervalValues.values.get(0));
        assertEquals(3, intervalValues.values.get(1));
        assertEquals(1, intervalValues.values.get(2));

        sortedStagingFile.close();
        metricIndex.close();
    }
}
