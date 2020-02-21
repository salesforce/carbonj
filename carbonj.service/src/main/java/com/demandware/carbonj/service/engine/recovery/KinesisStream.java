/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import java.util.Date;
import java.util.Set;

public interface KinesisStream {

    Set<ShardInfo> getShards();

    RecordAndIterator getNextRecord(ShardInfo shardInfo, String shardIterator, String lastSequenceNumber) throws InterruptedException;

    String getShardIterator(ShardInfo shardInfo, Date startTimeStamp);

    String getShardIterator(ShardInfo shardInfo, String sequenceNumber);
}
