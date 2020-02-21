/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class FetchLogAnalyzer {

    private final File file;

    FetchLogAnalyzer(File file) {

        this.file = file;
    }

    public void analyze() throws IOException {
        Map<String, Integer> secondToCount = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {

            String line = reader.readLine();
            while (line != null) {
                String second = parse(line);
                Integer counts = secondToCount.get(second);
                if (counts == null) {
                    counts = 0;
                }
                secondToCount.put(second, counts + 1);
                line = reader.readLine();
            }
        }
        Iterator<String> iterator = secondToCount.keySet().iterator();
        /*String[] minSecond = iterator.next().split(":");
        int prevMin = Integer.parseInt(minSecond[0]);
        int prevSec = Integer.parseInt(minSecond[1]); */
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.printf("%s -> %d\n", key, secondToCount.get(key));
        }
    }

    // 2019-10-04,14:39:02.176
    private String parse(String line) {
        int colonIndex = line.indexOf(':');
        int dotIndex = line.indexOf('.');
        return line.substring(colonIndex + 1, dotIndex);
    }

    public static void main(String[] args) throws IOException {
        new FetchLogAnalyzer(new File("/tmp/fetch.log")).analyze();
    }
}
