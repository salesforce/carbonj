/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KinesisStreamImpl implements KinesisStream {

    private static final Logger log = LoggerFactory.getLogger(KinesisStreamImpl.class);

    private static Timer timer;
    private final KinesisClient kinesis;
    private final String streamName;
    private final long retryTimeInMillis;

    KinesisStreamImpl(MetricRegistry metricRegistry, KinesisClient kinesis, String streamName, long retryTimeInMillis) {
        timer = metricRegistry.timer(MetricRegistry.name("KinesisStream", "getNextRecord"));
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.retryTimeInMillis = retryTimeInMillis;
    }

    @Override
    public Set<ShardInfo> getShards() {
        DescribeStreamRequest request = DescribeStreamRequest.builder().streamName(streamName).build();
        DescribeStreamResponse result = kinesis.describeStream(request);
        Set<ShardInfo> shardInfos = new HashSet<>();
        for (Shard shard: result.streamDescription().shards()) {
            shardInfos.add(new ShardInfoImpl(shard.shardId()));
        }
        return shardInfos;
    }

    @Override  // todo: improve error handling...
    public RecordAndIterator getNextRecord(ShardInfo shardInfo, String shardIterator, String lastSequenceNumber)
            throws InterruptedException {

        try (Timer.Context ignored = timer.time()) {
            GetRecordsRequest request = GetRecordsRequest.builder().limit(1).shardIterator(shardIterator).build();
            GetRecordsResponse result;

            try {
                result = kinesis.getRecords(request);
            } catch (ExpiredIteratorException e) {
                log.warn("Got expired iterator: {}", e.getMessage());

                // retry with last sequence number
                log.info("Trying with a new iterator");
                shardIterator = getShardIterator(shardInfo, lastSequenceNumber);
                request = GetRecordsRequest.builder().limit(1).shardIterator(shardIterator).build();
                result = kinesis.getRecords(request);
            }

            List<Record> records = result.records();
            shardIterator = result.nextShardIterator();

            // retry until we get the records.
            while (records.isEmpty() && shardIterator != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Recovery: Records not available yet for {}.  Waiting..", shardInfo.getShardId());
                }
                Thread.sleep(retryTimeInMillis);

                request = GetRecordsRequest.builder().limit(1).shardIterator(shardIterator).build();
                result = kinesis.getRecords(request);
                records = result.records();
                shardIterator = result.nextShardIterator();
            }

            if (records.isEmpty()) {  // end of shard has been reached.
                return RecordAndIterator.EMPTY;
            }
            return new RecordAndIterator(records.get(0), shardIterator);
        }
    }

    @Override
    public String getShardIterator(ShardInfo shardInfo, Date startTimeStamp) {
        GetShardIteratorRequest request = GetShardIteratorRequest.builder().streamName(streamName)
                .shardId(shardInfo.getShardId())
                .shardIteratorType(ShardIteratorType.AT_TIMESTAMP)
                .timestamp(startTimeStamp.toInstant())
                .build();
        GetShardIteratorResponse result = kinesis.getShardIterator(request);
        return result.shardIterator();
    }

    @Override
    public String getShardIterator(ShardInfo shardInfo, String sequenceNumber) {
        GetShardIteratorRequest request = GetShardIteratorRequest.builder().streamName(streamName)
                .shardId(shardInfo.getShardId())
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .startingSequenceNumber(sequenceNumber)
                .build();
        GetShardIteratorResponse result = kinesis.getShardIterator(request);
        return result.shardIterator();
    }
}
