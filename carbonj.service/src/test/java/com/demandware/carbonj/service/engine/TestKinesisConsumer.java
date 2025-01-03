/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.CLOUDWATCH;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

@Testcontainers
public class TestKinesisConsumer {

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:1.4.0")).withServices(KINESIS, CLOUDWATCH);

    private static final String STREAM_NAME = "test-stream";
    private static KinesisClient kinesisClient;

    @BeforeAll
    static void setUp() throws Exception {
        setEnvironmentVariable("AWS_ACCESS_KEY_ID", "accessKey");
        setEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "secretKey");

        kinesisClient = KinesisClient.builder()
                .endpointOverride(localstack.getEndpointOverride(KINESIS))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("accessKey", "secretKey")))
                .build();

        // Create the stream
        kinesisClient.createStream(builder -> builder.streamName(STREAM_NAME).shardCount(1));
        boolean isActive = false;
        while (!isActive) {
            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(STREAM_NAME)
                    .build();
            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);

            StreamStatus status = describeStreamResponse.streamDescription().streamStatus();
            if (status == StreamStatus.ACTIVE) {
                isActive = true;
            } else {
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void test() throws Exception {
        ListStreamsResponse listStreamsResponse = kinesisClient.listStreams();
        assertEquals(1, listStreamsResponse.streamNames().size());
        assertEquals(STREAM_NAME, listStreamsResponse.streamNames().get(0));

        MetricRegistry metricRegistry = new MetricRegistry();
        KinesisConfig kinesisConfig = new KinesisConfig(true, false, 60000, 60000, 60000,
                1, Path.of("/tmp/checkpoint"), 60, 60, "recoveryProvider", 1, 1, 1000);
        FileCheckPointMgr checkPointMgr = new FileCheckPointMgr(Path.of("/tmp/checkpoint"), 5);
        PointProcessorMock pointProcessor = new PointProcessorMock();
        KinesisConsumer kinesisConsumer = new KinesisConsumer(metricRegistry, pointProcessor, pointProcessor,
                STREAM_NAME, STREAM_NAME + "-app", kinesisConfig, checkPointMgr, metricRegistry.counter("kinesis-consumer-counter"),
                Region.US_EAST_1.id(), localstack.getEndpointOverride(KINESIS).toString());
        Thread.sleep(30000);
        int current = (int) (System.currentTimeMillis() / 1000);
        DataPoints dataPoints = new DataPoints(List.of(new DataPoint("foo.bar", 123.45, current)), current);
        DataPointCodec dataPointCodec = new GzipDataPointCodec();
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(STREAM_NAME)
                .data(SdkBytes.fromByteArray(dataPointCodec.encode(dataPoints)))
                .partitionKey("1")
                .build();
        kinesisClient.putRecord(putRecordRequest);
        int count = 0;
        while (count < 30) {
            if (pointProcessor.getCounter() == 1) break;
            count++;
            Thread.sleep(1000);
        }
        assertTrue(count < 30);
        kinesisConsumer.dumpStats();
        kinesisConsumer.closeQuietly();
    }

    private static void setEnvironmentVariable(String key, String value) throws Exception {
        // Use reflection to modify the internal environment map
        Map<String, String> env = System.getenv();
        Field field = env.getClass().getDeclaredField("m");
        field.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> writableEnv = (Map<String, String>) field.get(env);
        writableEnv.put(key, value);
    }
}
