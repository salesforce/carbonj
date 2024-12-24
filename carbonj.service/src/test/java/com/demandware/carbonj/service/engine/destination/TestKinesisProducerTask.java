/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.DeleteResourcePolicyRequest;
import com.amazonaws.services.kinesis.model.DeleteResourcePolicyResult;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeLimitsRequest;
import com.amazonaws.services.kinesis.model.DescribeLimitsResult;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetResourcePolicyRequest;
import com.amazonaws.services.kinesis.model.GetResourcePolicyResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ListStreamConsumersRequest;
import com.amazonaws.services.kinesis.model.ListStreamConsumersResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.MergeShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutResourcePolicyRequest;
import com.amazonaws.services.kinesis.model.PutResourcePolicyResult;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamResult;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.SplitShardResult;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.amazonaws.services.kinesis.model.UpdateStreamModeRequest;
import com.amazonaws.services.kinesis.model.UpdateStreamModeResult;
import com.amazonaws.services.kinesis.waiters.AmazonKinesisWaiters;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestKinesisProducerTask {
    @Test
    public void test() {
        MetricRegistry metricRegistry = new MetricRegistry();
        Meter sent = metricRegistry.meter(MetricRegistry.name("kinesis", "sent"));
        Meter drop = metricRegistry.meter(MetricRegistry.name("kinesis", "drop"));
        Meter messagesSent = metricRegistry.meter(MetricRegistry.name("kinesis", "messagesSent"));
        Histogram messageSize = metricRegistry.histogram(MetricRegistry.name("kinesis", "messageSize"));
        Histogram dataPointsPerMessage = metricRegistry.histogram(MetricRegistry.name("kinesis", "dataPointsPerMessage"));
        KinesisProducerTask kinesisProducerTask = new KinesisProducerTask(metricRegistry, new MockAmazonKinesis(), "test-stream",
                List.of(new DataPoint("foo.bar", 123, 0, false)),
                sent,
                drop,
                messagesSent,
                messageSize,
                metricRegistry.meter(MetricRegistry.name("kinesis", "putRetry")),
                metricRegistry.timer(MetricRegistry.name("kinesis", "producer")).time(),
                dataPointsPerMessage,
                new GzipDataPointCodec());
        kinesisProducerTask.run();
        assertEquals(1, messageSize.getCount());
        assertEquals(1, sent.getCount());
        assertEquals(0, drop.getCount());
        assertEquals(1, dataPointsPerMessage.getCount());
        assertEquals(1, messagesSent.getCount());
    }

    private static class MockAmazonKinesis implements AmazonKinesis {

        @Override
        public void setEndpoint(String s) {

        }

        @Override
        public void setRegion(Region region) {

        }

        @Override
        public AddTagsToStreamResult addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
            return null;
        }

        @Override
        public CreateStreamResult createStream(CreateStreamRequest createStreamRequest) {
            return null;
        }

        @Override
        public CreateStreamResult createStream(String s, Integer integer) {
            return null;
        }

        @Override
        public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
            return null;
        }

        @Override
        public DeleteResourcePolicyResult deleteResourcePolicy(DeleteResourcePolicyRequest deleteResourcePolicyRequest) {
            return null;
        }

        @Override
        public DeleteStreamResult deleteStream(DeleteStreamRequest deleteStreamRequest) {
            return null;
        }

        @Override
        public DeleteStreamResult deleteStream(String s) {
            return null;
        }

        @Override
        public DeregisterStreamConsumerResult deregisterStreamConsumer(DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
            return null;
        }

        @Override
        public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
            return null;
        }

        @Override
        public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
            return null;
        }

        @Override
        public DescribeStreamResult describeStream(String s) {
            return null;
        }

        @Override
        public DescribeStreamResult describeStream(String s, String s1) {
            return null;
        }

        @Override
        public DescribeStreamResult describeStream(String s, Integer integer, String s1) {
            return null;
        }

        @Override
        public DescribeStreamConsumerResult describeStreamConsumer(DescribeStreamConsumerRequest describeStreamConsumerRequest) {
            return null;
        }

        @Override
        public DescribeStreamSummaryResult describeStreamSummary(DescribeStreamSummaryRequest describeStreamSummaryRequest) {
            return null;
        }

        @Override
        public DisableEnhancedMonitoringResult disableEnhancedMonitoring(DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
            return null;
        }

        @Override
        public EnableEnhancedMonitoringResult enableEnhancedMonitoring(EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
            return null;
        }

        @Override
        public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
            return null;
        }

        @Override
        public GetResourcePolicyResult getResourcePolicy(GetResourcePolicyRequest getResourcePolicyRequest) {
            return null;
        }

        @Override
        public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
            return null;
        }

        @Override
        public GetShardIteratorResult getShardIterator(String s, String s1, String s2) {
            return null;
        }

        @Override
        public GetShardIteratorResult getShardIterator(String s, String s1, String s2, String s3) {
            return null;
        }

        @Override
        public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
            return null;
        }

        @Override
        public ListShardsResult listShards(ListShardsRequest listShardsRequest) {
            return null;
        }

        @Override
        public ListStreamConsumersResult listStreamConsumers(ListStreamConsumersRequest listStreamConsumersRequest) {
            return null;
        }

        @Override
        public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
            return null;
        }

        @Override
        public ListStreamsResult listStreams() {
            return null;
        }

        @Override
        public ListStreamsResult listStreams(String s) {
            return null;
        }

        @Override
        public ListStreamsResult listStreams(Integer integer, String s) {
            return null;
        }

        @Override
        public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
            return null;
        }

        @Override
        public MergeShardsResult mergeShards(MergeShardsRequest mergeShardsRequest) {
            return null;
        }

        @Override
        public MergeShardsResult mergeShards(String s, String s1, String s2) {
            return null;
        }

        @Override
        public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
            return new PutRecordResult();
        }

        @Override
        public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1) {
            return null;
        }

        @Override
        public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1, String s2) {
            return null;
        }

        @Override
        public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
            return null;
        }

        @Override
        public PutResourcePolicyResult putResourcePolicy(PutResourcePolicyRequest putResourcePolicyRequest) {
            return null;
        }

        @Override
        public RegisterStreamConsumerResult registerStreamConsumer(RegisterStreamConsumerRequest registerStreamConsumerRequest) {
            return null;
        }

        @Override
        public RemoveTagsFromStreamResult removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
            return null;
        }

        @Override
        public SplitShardResult splitShard(SplitShardRequest splitShardRequest) {
            return null;
        }

        @Override
        public SplitShardResult splitShard(String s, String s1, String s2) {
            return null;
        }

        @Override
        public StartStreamEncryptionResult startStreamEncryption(StartStreamEncryptionRequest startStreamEncryptionRequest) {
            return null;
        }

        @Override
        public StopStreamEncryptionResult stopStreamEncryption(StopStreamEncryptionRequest stopStreamEncryptionRequest) {
            return null;
        }

        @Override
        public UpdateShardCountResult updateShardCount(UpdateShardCountRequest updateShardCountRequest) {
            return null;
        }

        @Override
        public UpdateStreamModeResult updateStreamMode(UpdateStreamModeRequest updateStreamModeRequest) {
            return null;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
            return null;
        }

        @Override
        public AmazonKinesisWaiters waiters() {
            return null;
        }
    }
}
