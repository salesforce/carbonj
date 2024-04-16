/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

public class MetricUtils {
    public static String dbWriteTimerName(String dbName) {
        return "db." + dbName + ".write.time";
    }

    public static String dbBatchWriteTimerName(String dbName) {
        return "db." + dbName + ".batchWrite.time";
    }

    public static String dbReadTimerName(String dbName) {
        return "db." + dbName + ".read.time";
    }

    public static String dbDeleteTimerName(String dbName) {
        return "db." + dbName + ".delete.time";
    }

    public static String dbCatchUpTimerName(String dbName) {
        return "db." + dbName + ".catchup.time";
    }

    public static String dbCatchUpTimerErrorName(String dbName) {
        return "db." + dbName + ".catchup.error";
    }

    public static String dbSavedRecordsMeterName(String dbName) {
        return "db." + dbName + ".records.saved";
    }

    public static String dbEmptyReadTimerName(String dbName) {
        return "db." + dbName + ".emptyRead.time";
    }

    public static String dbDataPointLatencyName(String dbName, String namespace) {
        return "db." + dbName + ".namespace." + namespace + ".latency";
    }
}
