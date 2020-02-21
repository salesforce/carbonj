/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
//package com.demandware.carbonj.service.util;
//
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.JUnit4;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//
//@RunWith(JUnit4.class)
//public class ParseQueryLog {
//
////    @Test
////    public void parse() throws Exception {
////        Path path = Paths.get("/Users", "sponnusamy", "work", "query.log");
////        try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
////            String line = br.readLine();
////            while (line != null) {
////                String[] splits = line.split(" ");
////                if (splits.length > 2) {
////                    int time = Integer.parseInt(splits[splits.length - 1]);
////                    if (time > 5000) {
////                        System.out.println(line);
////                    }
////                }
////                line = br.readLine();
////            }
////        }
////    }
//}
