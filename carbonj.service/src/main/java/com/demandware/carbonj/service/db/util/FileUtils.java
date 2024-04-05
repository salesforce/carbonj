/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils
{
    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    public static File writableDir(String path)
    {
        Preconditions.checkState( StringUtils.isNotEmpty(path), "Dir path is empty" );
        File dir = new File( path );
        Preconditions.checkState( dir.exists(), "Dir [" + dir.getAbsolutePath() + "] doesnt' exist.");
        Preconditions.checkState( dir.canRead(), "Do not have permissions to read from " + dir.getAbsolutePath() );
        Preconditions.checkState( dir.canWrite(), "Do not have permissions to write to " + dir.getAbsolutePath() );
        return dir;
    }

    public static File getSyncDirFromDbDir(File dbDir) {
        return new File(dbDir.getParentFile(), dbDir.getName() + "-sync");
    }

    public static void dumpQueueToFile(Queue<String> queue, File file) throws IOException {
        if (queue.isEmpty()) {
            return;
        }
        int size = queue.size();
        Set<String> lines = new HashSet<>();
        while (size-- > 0) {
            lines.add(queue.poll());
        }
        dumpSetToFile(lines, file);
    }

    public static void dumpSetToFile(Set<String> lines, File file) throws IOException {
        if (lines.isEmpty()) {
            return;
        }
        File parent = file.getParentFile();
        if (!parent.exists()) {
            log.info("Creating directory {}", parent.getAbsolutePath());
            if (!parent.mkdir()) {
                log.error("Failed to create directory {}", parent.getAbsolutePath());
                log.error("Unable to dump {}", StringUtils.join(lines, ','));
                return;
            }
        }
        if (!parent.canWrite()) {
            log.error("Cannot write to directory {}", parent.getAbsolutePath());
            log.error("Unable to dump {}", StringUtils.join(lines, ','));
            return;
        }
        log.info("Dumping queue with {} elements into file {}", lines.size(), file.getAbsoluteFile());
        org.apache.commons.io.FileUtils.writeLines(file, StandardCharsets.UTF_8.name(), lines, false);
    }

    public static Set<String> readFilesToSet(File syncDir, String prefix, boolean delete) {
        Set<String> results = new TreeSet<>();
        if (!syncDir.exists()) {
            log.error("Directory {} does not exist", syncDir.getAbsolutePath());
            return results;
        }
        if (!syncDir.canRead()) {
            log.error("Cannot read from directory {}", syncDir.getAbsolutePath());
            return results;
        }

        File[] syncFiles = syncDir.listFiles((dir, name) -> name.startsWith(prefix));
        if (syncFiles == null || syncFiles.length == 0) {
            log.info("No file with prefix {} to sync under directory {}", prefix, syncDir);
            return results;
        }
        for (File syncFile : syncFiles) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(syncFile))) {
                log.info("Syncing file from {}", syncFile.getAbsolutePath());
                String nameIndex;
                while ((nameIndex = bufferedReader.readLine()) != null) {
                    results.add(nameIndex.trim());
                }
            } catch (IOException e) {
                log.error("Failed to sync file from {} - {}", syncFile.getAbsolutePath(), e.getMessage(), e);
                continue;
            }
            if (delete) {
                if (!syncFile.delete()) {
                    log.error("Failed to delete file {}", syncFile.getAbsolutePath());
                }
            }
        }
        return results;
    }
}
