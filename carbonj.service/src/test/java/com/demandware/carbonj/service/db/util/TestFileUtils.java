/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFileUtils {
    @Test
    public void testNegatives() throws Exception {
        FileUtils.dumpQueueToFile(new ArrayDeque<>(), new File("/tmp/test"));
        Set<String> lines = new HashSet<>();
        FileUtils.dumpSetToFile(lines, new File("/tmp/test"));
        lines.add("test line");
        FileUtils.dumpSetToFile(lines, new File("/cannot_create/dump.txt"));
        assertFalse(new File("/cannot_create/dump.txt").exists());
        FileUtils.dumpSetToFile(lines, new File("/dump.txt"));
        assertFalse(new File("/dump.txt").exists());
        assertTrue(FileUtils.readFilesToSet(new File("/does_not_exist"), "prefix", false).isEmpty());
    }
}
