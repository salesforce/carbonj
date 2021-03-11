/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KinesisStreamImpl implements KinesisStream {

    private static final Logger log = LoggerFactory.getLogger(KinesisStreamImpl.class);

    private static Timer timer;
    private final AmazonKinesis kinesis;
    private final String streamName;
    private final long retryTimeInMillis;

    KinesisStreamImpl(MetricRegistry metricRegistry, AmazonKinesis kinesis, String streamName, long retryTimeInMillis) {
        this.timer = metricRegistry.timer(MetricRegistry.name("KinesisStream", "getNextRecord"));
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.retryTimeInMillis = retryTimeInMillis;
    }

    @Override
    public Set<ShardInfo> getShards() {
        DescribeStreamRequest request = new DescribeStreamRequest().withStreamName(streamName);
        DescribeStreamResult result = kinesis.describeStream(request);
        Set<ShardInfo> shardInfos = new HashSet<>();
        for (Shard shard: result.getStreamDescription().getShards()) {
            shardInfos.add(new ShardInfoImpl(shard.getShardId()));
        }
        return shardInfos;
    }

    @Override  // todo: improve error handling...
    public RecordAndIterator getNextRecord(ShardInfo shardInfo, String shardIterator, String lastSequenceNumber)
            throws InterruptedException {
        Timer.Context context = timer.time();

        try {
            GetRecordsRequest request = new GetRecordsRequest().withLimit(1).withShardIterator(shardIterator);
            GetRecordsResult result;

            try {
                result = kinesis.getRecords(request);
            } catch (ExpiredIteratorException e) {
                log.warn("Got expired iterator: ", e.getMessage());

                // retry with last sequence number
                log.info("Trying with a new iterator");
                shardIterator = getShardIterator(shardInfo, lastSequenceNumber);
                request = new GetRecordsRequest().withLimit(1).withShardIterator(shardIterator);
                result = kinesis.getRecords(request);
            }

            List<Record> records = result.getRecords();
            shardIterator = result.getNextShardIterator();

            // retry until we get the records.
            while (records.size() == 0 && shardIterator != null) {
                if (log.isDebugEnabled())
                {
                    log.debug( "Recovery: Records not available yet for " + shardInfo.getShardId() + ".  Waiting.." );
                }
                Thread.sleep(retryTimeInMillis);

                request = new GetRecordsRequest().withLimit(1).withShardIterator(shardIterator);
                result = kinesis.getRecords(request);
                records = result.getRecords();
                shardIterator = result.getNextShardIterator();
            }

            if (records.size() == 0) {  // end of shard has been reached.
                return RecordAndIterator.EMPTY;
            }
            return new RecordAndIterator(records.get(0), shardIterator);
        } finally {
            context.close();
        }
    }

    @Override
    public String getShardIterator(ShardInfo shardInfo, Date startTimeStamp) {
        GetShardIteratorRequest request = new GetShardIteratorRequest().withStreamName(streamName)
                .withShardId(shardInfo.getShardId())
                .withShardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                .withTimestamp(startTimeStamp);
        GetShardIteratorResult result = kinesis.getShardIterator(request);
        return result.getShardIterator();
    }

    @Override
    public String getShardIterator(ShardInfo shardInfo, String sequenceNumber) {
        GetShardIteratorRequest request = new GetShardIteratorRequest().withStreamName(streamName)
                .withShardId(shardInfo.getShardId())
                .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .withStartingSequenceNumber(sequenceNumber);
        GetShardIteratorResult result = kinesis.getShardIterator(request);
        return result.getShardIterator();
    }
}
