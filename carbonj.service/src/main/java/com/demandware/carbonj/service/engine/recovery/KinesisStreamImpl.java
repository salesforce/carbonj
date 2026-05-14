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

import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class KinesisStreamImpl implements KinesisStream {

    private static final Logger log = LoggerFactory.getLogger(KinesisStreamImpl.class);

    private static Timer timer;
    private final KinesisClient kinesis;
    private final String streamName;
    private final long retryTimeInMillis;
    private final long minFetchIntervalMillis;
    private final int getRecordsLimit;

    // Per-shard buffer of records pre-fetched by a single batched GetRecords call.
    // GapProcessor consumes one record at a time; buffering lets us amortize the
    // 5 GetRecords/sec/shard quota across many records instead of one call per record.
    private final Map<String, ShardBuffer> buffers = new HashMap<>();

    KinesisStreamImpl(MetricRegistry metricRegistry, KinesisClient kinesis, String streamName, long retryTimeInMillis,
                      long minFetchIntervalMillis, int getRecordsLimit) {
        timer = metricRegistry.timer(MetricRegistry.name("KinesisStream", "getNextRecord"));
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.retryTimeInMillis = retryTimeInMillis;
        this.minFetchIntervalMillis = minFetchIntervalMillis;
        this.getRecordsLimit = getRecordsLimit;
    }

    private static final class ShardBuffer {
        final Deque<Record> records = new ArrayDeque<>();
        // The shardIterator we surface to the caller. The caller round-trips this
        // value back into getNextRecord on subsequent calls. Recognising it on
        // re-entry is how we know we should drain the buffer instead of reseeding.
        String currentIterator;
        // The shardIterator to use for the next batched GetRecords call.
        String nextIterator;
        // Wall-clock millis when this shard's most recent GetRecords completed.
        // Used to enforce minFetchIntervalMillis between calls so recovery
        // does not exhaust the per-shard 5 GetRecords/sec/shard quota and
        // starve the live consumer.
        long lastFetchMillis = 0L;
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
            ShardBuffer buffer = buffers.computeIfAbsent(shardInfo.getShardId(), k -> new ShardBuffer());

            // If the caller passes an iterator that is not the one we last surfaced,
            // it is a fresh seed (initial call, or a recovery iterator from
            // getShardIterator after an ExpiredIteratorException). Reset the buffer.
            if (!Objects.equals(shardIterator, buffer.currentIterator)) {
                buffer.records.clear();
                buffer.currentIterator = shardIterator;
                buffer.nextIterator = shardIterator;
            }

            // Refill the buffer if empty. Loop because Kinesis may return an empty
            // batch even when more records exist downstream of nextIterator.
            while (buffer.records.isEmpty() && buffer.nextIterator != null) {
                GetRecordsResponse result = fetchBatch(shardInfo, buffer, lastSequenceNumber);
                buffer.nextIterator = result.nextShardIterator();
                if (!result.records().isEmpty()) {
                    buffer.records.addAll(result.records());
                    break;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Recovery: Records not available yet for {}.  Waiting..", shardInfo.getShardId());
                }
                Thread.sleep(retryTimeInMillis);
            }

            if (buffer.records.isEmpty()) {  // end of shard has been reached.
                buffers.remove(shardInfo.getShardId());
                return RecordAndIterator.EMPTY;
            }

            Record record = buffer.records.poll();
            // Surface nextIterator to the caller so they round-trip it back; the
            // equality check above will recognise it and keep draining the buffer.
            buffer.currentIterator = buffer.nextIterator;
            return new RecordAndIterator(record, buffer.nextIterator);
        }
    }

    private GetRecordsResponse fetchBatch(ShardInfo shardInfo, ShardBuffer buffer, String lastSequenceNumber)
            throws InterruptedException {
        // Cap recovery's GetRecords rate per shard. Kinesis enforces 5 GetRecords/sec/shard;
        // the live KCL consumer also reads from the same shard at ~1/sec. Without this gate,
        // recovery's tight per-record loop can saturate the quota and trigger
        // ProvisionedThroughputExceededException for both paths.
        if (minFetchIntervalMillis > 0 && buffer.lastFetchMillis > 0) {
            long elapsed = System.currentTimeMillis() - buffer.lastFetchMillis;
            long wait = minFetchIntervalMillis - elapsed;
            if (wait > 0) {
                Thread.sleep(wait);
            }
        }
        GetRecordsRequest request = GetRecordsRequest.builder()
                .limit(getRecordsLimit)
                .shardIterator(buffer.nextIterator)
                .build();
        try {
            return kinesis.getRecords(request);
        } catch (ExpiredIteratorException e) {
            log.warn("Got expired iterator: {}", e.getMessage());
            log.info("Trying with a new iterator");
            String newIterator = getShardIterator(shardInfo, lastSequenceNumber);
            buffer.nextIterator = newIterator;
            buffer.currentIterator = newIterator;
            return kinesis.getRecords(GetRecordsRequest.builder()
                    .limit(getRecordsLimit)
                    .shardIterator(newIterator)
                    .build());
        } finally {
            buffer.lastFetchMillis = System.currentTimeMillis();
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
