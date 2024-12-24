/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSystemSort {
    @Test
    public void testSystemSort() throws Exception {
        SystemSort systemSort = new SystemSort();
        String metric1 = "5980273329 6.29 pod279.gm.prd.util1.api.ServiceHelper.isCloudV1Instance.min";
        String metric2 = "5978422979 228.19 pod279.gm.prd.util1.api.ServiceHelper.isInAdminMode.max";
        File stage1 = new File("/tmp/test_stage1");
        FileUtils.writeLines(stage1, List.of(metric1));
        File stage2 = new File("/tmp/test_stage2");
        FileUtils.writeLines(stage2, List.of(metric2));
        File stageSorted = new File("/tmp/test_stage_sorted");
        systemSort.sort(stage1, Optional.of(stage2), stageSorted);
        List<String> lines = FileUtils.readLines(stageSorted, StandardCharsets.UTF_8);
        assertEquals(metric2, lines.get(0));
        assertEquals(metric1, lines.get(1));
    }
}
