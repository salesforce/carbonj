/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileSystemGapsTableImpl implements GapsTable {

    private static final String VERSION = "1.0";

    private File gapFile;

    public FileSystemGapsTableImpl(Path rootDir) throws Exception {
        if (!Files.exists(rootDir)) {
            Files.createDirectories(rootDir);
        }
        this.gapFile = Paths.get(rootDir.toString(), "gaps.txt").toFile();
    }

    @Override
    public void add(Gap gap) throws Exception {
        List<Gap> gaps = getGaps();
        gaps.add(gap);
        persist(gaps);
    }

    private void persist(List<Gap> gaps) throws FileNotFoundException {
        try (PrintWriter pw = new PrintWriter(gapFile)) {
            pw.println(VERSION);
            for (Gap gap : gaps) {
                pw.println(String.format("%d,%d,%d", gap.startTime().getTime(), gap.endTime().getTime(), gap.lastRecovered().getTime()));
            }
        }
    }

    @Override
    public List<Gap> getGaps() throws Exception {
        List<Gap> gaps = new ArrayList<>();

        if (!gapFile.exists()) {
            return gaps;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(gapFile))) {
            br.readLine();  // version -- ignore for now..
            String line = br.readLine();
            while (line != null) {
                String[] splits = line.split(",");
                if (splits.length == 3) {
                    long startTime = Long.parseLong(splits[0]);
                    long endTime = Long.parseLong(splits[1]);
                    long lastRecoveredTime = Long.parseLong(splits[2]);
                    gaps.add(new GapImpl(new Date(startTime), new Date(endTime), new Date(lastRecoveredTime)));
                }
                line = br.readLine();
            }
            return gaps;
        }
    }

    @Override
    public void delete(Gap gap) throws Exception {
        List<Gap> gaps = getGaps();
        gaps.remove(gap);
        persist(gaps);
    }

    @Override
    public boolean updateGap(Gap gap) throws Exception {
        List<Gap> gaps = getGaps();
        for (int i = 0; i < gaps.size(); i++) {
            if (gaps.get(i).startTime().equals(gap.startTime())) {
                gaps.set(i, gap);
                persist(gaps);
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        gapFile.delete();
    }
}
