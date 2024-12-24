/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.index.IndexUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class TestStagingFile {
    @Test
    public void test() {
        MetricRegistry metricRegistry  = new MetricRegistry();
        File file = new File("/tmp/staging");
        StagingFile stagingFile = new StagingFile(metricRegistry, file, new SystemSort(), IndexUtils.metricIndex(new File("/tmp/testdb"), true));
        assertFalse(stagingFile.write(null));
        assertEquals("StagingFile{file=/tmp/staging}", stagingFile.toString());
        assertNotEquals(0, stagingFile.hashCode());
        assertTrue(stagingFile.equals(stagingFile));
        assertFalse(stagingFile.equals(new Object()));
        assertFalse(stagingFile.equals(new StagingFile(metricRegistry, new File("/tmp/staging2"), new SystemSort(), IndexUtils.metricIndex(new File("/tmp/testdb"), true))));
    }
}
