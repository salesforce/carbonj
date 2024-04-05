/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SyncPrimaryDbTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SyncPrimaryDbTask.class);
    private final RocksDB rocksDB;
    private final File dbDir;
    private final Timer catchUpTimer;
    private final Meter catchUpTimerError;
    private final int catchupRetry;

    public SyncPrimaryDbTask(RocksDB rocksDB, File dbDir, Timer catchUpTimer, Meter catchUpTimerError, int catchupRetry) {
        this.rocksDB = rocksDB;
        this.dbDir = dbDir;
        this.catchUpTimer = catchUpTimer;
        this.catchUpTimerError = catchUpTimerError;
        this.catchupRetry = catchupRetry;
    }

    @Override
    public void run() {
        int retry = 1;
        while (retry <= catchupRetry) {
            log.info("{}: Start syncing with primary DB {}", retry, dbDir.getAbsolutePath());
            try (Timer.Context ignored = catchUpTimer.time()) {
                rocksDB.tryCatchUpWithPrimary();
                break;
            } catch (RocksDBException e) {
                retry++;
                log.error("Failed to sync with primary DB {} - {}", dbDir.getAbsolutePath(), e.getMessage(), e);
            }
        }
        if (retry > catchupRetry) {
            catchUpTimerError.mark();
        } else {
            log.info("{}: Completed syncing with primary DB {}", retry, dbDir.getAbsolutePath());
        }
    }
}
