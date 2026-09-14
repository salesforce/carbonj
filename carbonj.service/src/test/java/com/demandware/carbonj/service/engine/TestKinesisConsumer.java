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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static com.demandware.carbonj.service.engine.TestUtils.setEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.*;

@Testcontainers
public class TestKinesisConsumer {

    private static final Logger log = LoggerFactory.getLogger(TestKinesisConsumer.class);

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4.7.0")).withServices(KINESIS, DYNAMODB, CLOUDWATCH);

    private static KinesisClient kinesisClient;
    private final List<KinesisConsumer> consumers = new ArrayList<>();

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

    }

    @AfterEach
    void tearDown() {
        consumers.forEach(KinesisConsumer::closeQuietly);
    }

    private static void createStream(String streamName, int shardCount) throws Exception {
        kinesisClient.createStream(builder -> builder.streamName(streamName).shardCount(shardCount));
        await(Duration.ofSeconds(30), () -> {
            DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(streamName)
                    .build();
            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);
            return describeStreamResponse.streamDescription().streamStatus() == StreamStatus.ACTIVE;
        });
    }

    @Test
    public void twoConsumersShareAllRecordsWithoutDuplicates() throws Exception {
        String testId = UUID.randomUUID().toString();
        String streamName = "test-stream-" + testId;
        String applicationName = "test-app-" + testId;
        createStream(streamName, 2);

        ListStreamsResponse listStreamsResponse = kinesisClient.listStreams();
        assertTrue(listStreamsResponse.streamNames().contains(streamName));

        MetricRegistry metricRegistry = new MetricRegistry();
        Path checkPointDir = Path.of("/tmp/checkpoint-" + testId);
        KinesisConfig kinesisConfig = new KinesisConfig(true, false, 1000, 1000, 1000,
                1, checkPointDir, 1, 2, "filesystem", 1, 100, 1000, "never", false);
        FileCheckPointMgr checkPointMgr = new FileCheckPointMgr(checkPointDir, 5);
        PointProcessorMock pointProcessor = new PointProcessorMock();
        consumers.add(new KinesisConsumer(metricRegistry, pointProcessor, pointProcessor,
                streamName, applicationName, kinesisConfig, checkPointMgr, metricRegistry.counter("kinesis-consumer-a"),
                Region.US_EAST_1.id(), 0, localstack.getEndpointOverride(KINESIS).toString()));
        consumers.add(new KinesisConsumer(metricRegistry, pointProcessor, pointProcessor,
                streamName, applicationName, kinesisConfig, checkPointMgr, metricRegistry.counter("kinesis-consumer-b"),
                Region.US_EAST_1.id(), 0, localstack.getEndpointOverride(KINESIS).toString()));

        Thread.sleep(3000);
        log.info("Start ingesting data points into {} ...", streamName);
        int current = (int) (System.currentTimeMillis() / 1000);
        DataPointCodec dataPointCodec = new GzipDataPointCodec();
        Set<String> expectedMetricNames = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String metricName = "multi.worker.test." + testId + "." + i;
            expectedMetricNames.add(metricName);
            DataPoints dataPoints = new DataPoints(List.of(new DataPoint(metricName, i, current)), current);
            PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                    .streamName(streamName)
                    .data(SdkBytes.fromByteArray(dataPointCodec.encode(dataPoints)))
                    .partitionKey(Integer.toString(i % 2))
                    .build();
            kinesisClient.putRecord(putRecordRequest);
        }

        await(Duration.ofSeconds(60), () -> pointProcessor.getCounter() >= expectedMetricNames.size());
        assertEquals(expectedMetricNames.size(), pointProcessor.getCounter(), "records must be processed exactly once");
        assertEquals(expectedMetricNames, pointProcessor.getMetricNames());
        consumers.forEach(KinesisConsumer::dumpStats);
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(250);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met within " + timeout);
    }
}
