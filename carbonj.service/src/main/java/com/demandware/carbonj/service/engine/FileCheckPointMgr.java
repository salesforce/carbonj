/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class FileCheckPointMgr implements CheckPointMgr<Date> {

    private static final Logger log = LoggerFactory.getLogger(FileCheckPointMgr.class);

    private static final String VERSION = "1.0";

    private final File checkPointFile;
    private final int defaultOffsetMins;

    public FileCheckPointMgr(Path checkPointDir, int defaultOffsetMins) throws Exception {
        this.defaultOffsetMins = defaultOffsetMins;
        if (Files.notExists(checkPointDir)) {
            Files.createDirectories(checkPointDir);
        }

        checkPointFile = Paths.get(checkPointDir.toString(), "checkpoint.txt").toFile();
    }

    @Override
    public void checkPoint(Date checkPoint) throws Exception {
        try (PrintWriter pw = new PrintWriter(checkPointFile)) {
            pw.println(VERSION);
            pw.println(String.valueOf(checkPoint.getTime()));
        }
    }

    @Override
    public Date lastCheckPoint() throws Exception {
        if (!checkPointFile.exists()) {
            return getDefaultCheckPoint();
        }

        try (BufferedReader br = new BufferedReader(new FileReader(checkPointFile))) {
            br.readLine();  // version -- ignore for now..
            String ts = br.readLine();
            if (ts != null) {
                return new Date(Long.parseLong(ts));
            } else {
                return getDefaultCheckPoint();
            }
        }
    }

    private Date getDefaultCheckPoint() {
        Date checkPoint = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(defaultOffsetMins));
        log.warn("Check point not found!  new checkpoint using default offset: " + checkPoint);
        return checkPoint;
    }
}

