/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import org.springframework.beans.factory.annotation.Value;

public class RocksDBConfig
{
    @Value( "${rocksdb.minWriteBufferNumberToMerge:1}" )
    int minWriteBufferNumberToMerge = 1;

    @Value( "${rocksdb.writeBufferSize:268435456}" )
    long writeBufferSize = 268435456;

    @Value( "${rocksdb.maxWriteBufferNumber:4}" )
    int maxWriteBufferNumber = 4;

    @Value( "${rocksdb.targetFileSizeBase:26843545}" )
    long targetFileSizeBase = 26843545;

    @Value( "${rocksdb.targetFileSizeMultiplier:10}")
    int targetFileSizeMultiplier = 10;

    @Value( "${rocksdb.levelZeroFileNumCompactionTrigger:1}")
    int levelZeroFileNumCompactionTrigger = 1;

    @Value( "${rocksdb.levelZeroSlowDownWriteTrigger:8}")
    int levelZeroSlowDownWritesTrigger = 8;

    @Value( "${rocksdb.levelZeroStopWritesTrigger:10}")
    int levelZeroStopWritesTrigger = 10;

    @Value( "${rocksdb.numLevels:5}")
    int numLevels = 5;

    @Value( "${rocksdb.maxBytesForLevelBase:268435456}")
    long maxBytesForLevelBase = 268435456;

    @Value( "${rocksdb.maxBytesForLevelMultiplier:8}")
    int maxBytesForLevelMultiplier = 8;

    @Value( "${rocksdb.increaseParallelism:4}")
    int increaseParallelism = 4;

    @Value( "${rocksdb.maxBackgroundCompactions:48}")
    int maxBackgroundCompactions = 48;

    @Value( "${rocksdb.maxBackgroundFlushes:4}")
    int maxBackgroundFlushes = 4;

    @Value( "${rocksdb.blockSize:32768}")  // 32K
    long blockSize = 32 * 1024;

    @Value("${rocksdb.blockCacheSize:33554432}") // 32M, 8388608
    long blockCacheSize = 33554432; //

    @Value("${rocksdb.compactionThreadPoolSize:48}")
    int compactionThreadPoolSize = 48;

    @Value("${rocksdb.flushThreadPoolSize:2}")
    int flushThreadPoolSize = 2;

    @Value("${rocksdb.useBlockBasedTableConfig:true}")
    boolean useBlockBasedTableConfig = true;

    @Value("${rocksdb.useBloomFilter:false}")
    boolean useBloomFilter = false;

    @Value("${rocksdb.disableWAL:false}")
    boolean disableWAL = false;

    @Value("${rocksdb.maxGrandparentOverlapFactor:-1}")
    int maxGrandparentOverlapFactor = -1;

    @Value("${rocksdb.sourceCompactionFactor:-1}")
    int sourceCompactionFactor = -1;

    @Value("${rocksdb.bytesPerSync:-1}")
    long bytesPerSync = -1;

    @Value("${rocksdb.readonly:false}")
    boolean readOnly = false;

    @Value("${rocksdb.catchup.retry:3}")
    int catchupRetry = 3;

    @Value("${rocksdb.object.cleaner.queue.size:100000}")
    int objectCleanerQueueSize = 100000;

    @Value("${rocksdb.keepLogFileNum:10}")
    int keepLogFileNum = 10;

    @Override
    public String toString()
    {
        return "RocksDBConfig{" +
                        "minWriteBufferNumberToMerge=" + minWriteBufferNumberToMerge +
                        ", writeBufferSize=" + writeBufferSize +
                        ", maxWriteBufferNumber=" + maxWriteBufferNumber +
                        ", targetFileSizeMultiplier=" + targetFileSizeMultiplier +
                        ", levelZeroFileNumCompactionTrigger=" + levelZeroFileNumCompactionTrigger +
                        ", disableWAL=" + disableWAL +
                        ", useBloomFilter=" + useBloomFilter +
                        ", readOnly=" + readOnly +
                        ", catchupRetry=" + catchupRetry +
                        ", objectCleanerQueueSize=" + objectCleanerQueueSize +
                        '}';
    }
}
