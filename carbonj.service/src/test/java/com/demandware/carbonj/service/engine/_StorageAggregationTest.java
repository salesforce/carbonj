/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.model.StorageAggregationPolicySource;
import org.joda.time.DateTime;
import org.junit.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertTrue;

public class _StorageAggregationTest {

    private static Path storageAggregationRulesFilePath;

    private static Path carbonjDataDirPath;

    private static Path carbonjStagingDirPath;

    private static Path carbonjCheckpointDirPath;

    private static final String DB_5M = "5m7d";

    @BeforeClass
    public static void start() throws IOException {
        // Create conf for storage aggregation rules.
        storageAggregationRulesFilePath = Paths.get("/tmp/storage-aggregation.conf");
        File storageAggregationRulesFile = storageAggregationRulesFilePath.toFile();
        //noinspection ResultOfMethodCallIgnored
        storageAggregationRulesFile.delete();
        setupStorageAggregationRules(storageAggregationRulesFile);

        // Create carbonj data dir
        carbonjDataDirPath = Paths.get("/tmp/carbonj-data");
        File carbonjDataDirFile = carbonjDataDirPath.toFile();
        deleteDirRecursivelyIfExists(carbonjDataDirPath);
        assertTrue("Failed to create carbonj data dir: " + carbonjDataDirPath.toString(), carbonjDataDirFile.mkdirs());

        // Create carbonj staging dir
        carbonjStagingDirPath = Paths.get("/tmp/carbonj-staging");
        File carbonjStagingDirFile = carbonjStagingDirPath.toFile();
        deleteDirRecursivelyIfExists(carbonjStagingDirPath);
        assertTrue("Failed to create carbonj staging dir: " + carbonjStagingDirPath.toString(), carbonjStagingDirFile.mkdirs());

        // Create carbonj checkpoint dir
        carbonjCheckpointDirPath = Paths.get("/tmp/carbonj-checkpoint");
        File carbonjCheckpointDirFile = carbonjCheckpointDirPath.toFile();
        deleteDirRecursivelyIfExists(carbonjCheckpointDirPath);
        assertTrue("Failed to create carbonj checkpoint dir: " + carbonjCheckpointDirPath.toString(), carbonjCheckpointDirFile.mkdirs());

        // Start carbonj
        Map<String, String> properties = new HashMap<>();
        properties.put("metrics.store.enabled", "true");
        properties.put("storage.aggregation.rules", storageAggregationRulesFilePath.toString());
        properties.put("metrics.store.dataDir", carbonjDataDirPath.toString());
        properties.put("metrics.store.stagingDir", carbonjStagingDirPath.toString());
        properties.put("metrics.store.checkPoint.dir", carbonjCheckpointDirPath.toString());
        properties.put("staging.systemSort.nParallel", "0");
//        startCarbonJ(Collections.unmodifiableMap(properties));
    }

    @AfterClass
    public static void stop() throws IOException {
        Files.deleteIfExists(storageAggregationRulesFilePath);
        deleteDirRecursivelyIfExists(carbonjDataDirPath);
        deleteDirRecursivelyIfExists(carbonjStagingDirPath);
        deleteDirRecursivelyIfExists(carbonjCheckpointDirPath);
    }

    private static void setupStorageAggregationRules(File file) throws IOException {
        final String sumRule = "sum = .*\\Qstorage.agg.test.sum\\E$";
        final String minRule = "min = .*\\Qstorage.agg.test.min\\E$";
        final String maxRule = "max = .*\\Qstorage.agg.test.max\\E$";
        final String avgRule = "avg = .*\\Qstorage.agg.test.avg\\E$";
        final String catchAllRule = "avg = *";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(sumRule);
            bw.newLine();
            bw.write(minRule);
            bw.newLine();
            bw.write(maxRule);
            bw.newLine();
            bw.write(avgRule);
            bw.newLine();
            bw.write(catchAllRule);
            bw.newLine();
        }
    }

    @Test
    @Ignore("Not ready until run time can be reduced.")
    public void testStorageAggregationMethods() throws InterruptedException {
//        StorageAggregationPolicySource policySource = carbonj.getRuntime().__getInjectedApplicationContext()
//                .getBean("storageAggregationPolicySource", StorageAggregationPolicySource.class);
//        policySource.getRulesLoader().reload();
//        Assert.assertEquals("Invalid aggregation rules.", 5, policySource.getRulesLoader().getRules().size());
//
//        Map<DateTime, Double> dateTimeValueMap = getDateTimeValueMap(15);
//        List<String> metrics = Arrays.asList("metric1.storage.agg.test.sum", "metric2.storage.agg.test.min", "metric3.storage.agg.test.max",
//                "metric4.storage.agg.test.avg", "metric5.value");
//        metrics.forEach(metric -> {
//            dateTimeValueMap.forEach((key, value) -> cjClient.send(metric, value, key));
//        });
//
//        drain();
//
//        // TODO reduce wait time - it is large because the staging files don't get sorted until the files are left unmodified for 90 seconds.
//        Thread.sleep(120 * 1000);
//
//        boolean foundValue = cjClient.listPoints(metrics.get(0), DB_5M).entrySet().stream()
//                .anyMatch(e -> e.getValue().equals(Double.valueOf(10)));
//        Assert.assertTrue("Sum aggregation failed.", foundValue);
//
//        foundValue = cjClient.listPoints(metrics.get(1), DB_5M).entrySet().stream()
//                .anyMatch(e -> e.getValue().equals(Double.valueOf(0)));
//        Assert.assertTrue("Min aggregation failed.", foundValue);
//
//        foundValue = cjClient.listPoints(metrics.get(2), DB_5M).entrySet().stream()
//                .anyMatch(e -> e.getValue().equals(Double.valueOf(4)));
//        Assert.assertTrue("Max aggregation failed.", foundValue);
//
//        foundValue = cjClient.listPoints(metrics.get(3), DB_5M).entrySet().stream()
//                .anyMatch(e -> e.getValue().equals(Double.valueOf(2)));
//        Assert.assertTrue("Avg aggregation failed.", foundValue);
//
//        foundValue = cjClient.listPoints(metrics.get(4), DB_5M).entrySet().stream()
//                .anyMatch(e -> e.getValue().equals(Double.valueOf(2)));
//        Assert.assertTrue("Avg aggregation failed.", foundValue);
    }

    /**
     * Generates data series in the following manner:
     * dt0 dt1 dt2 dt3 dt4 dt5 dt6 dt7 ...
     * 0   1   2   3   4   0   1   2 ...
     */
    private Map<DateTime, Double> getDateTimeValueMap(int mins) {
        DateTime now = new DateTime();
        return IntStream.range(0, mins).boxed()
                .collect(toMap(min -> new DateTime(now).minusMinutes(min).withSecondOfMinute(0).withMillisOfSecond(0),
                        min -> Double.valueOf(min % 5)));
    }

    private static void deleteDirRecursivelyIfExists(Path dirPath) throws IOException {
        if (Files.exists(dirPath)) {
            Files.walk(dirPath)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignore) {

                        }
                    });
        }
    }
}
