/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;

import java.time.Duration;

@Configuration
public class cfgAws {
    private static final int INITIAL_WINDOW_SIZE_BYTES = 512 * 1024; // 512 KB
    private static final long HEALTH_CHECK_PING_PERIOD_MILLIS = 60 * 1000;

    @Value("${aws.region:us-east-1}")
    private String region;

    @Value("#{new Long('${aws.kinesis.processor.requestTimeoutMillis:30000}')}")
    private long kinesisProcessorRequestTimeoutMillis;

    @Bean
    @Profile("!test")
    public AwsCredentialsProvider awsCredentialsProvider() {
        return InstanceProfileCredentialsProvider.builder().build();
    }

    @Bean
    @Profile("!test")
    public KinesisAsyncClient kinesisAsyncClient(AwsCredentialsProvider awsCredentialsProvider) {
        // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/http-configuration-netty.html
        KinesisAsyncClientBuilder kinesisAsyncClientBuilder = KinesisAsyncClient.builder()
                .credentialsProvider(awsCredentialsProvider).region(Region.of(region));
        return kinesisAsyncClientBuilder.httpClientBuilder(
                        NettyNioAsyncHttpClient.builder()
                                .connectionTimeout(Duration.ofMillis(kinesisProcessorRequestTimeoutMillis))
                                .readTimeout(Duration.ofMillis(kinesisProcessorRequestTimeoutMillis))
                                .maxConcurrency(Integer.MAX_VALUE)
                                .http2Configuration(Http2Configuration.builder().initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
                                        .healthCheckPingPeriod(Duration.ofMillis(HEALTH_CHECK_PING_PERIOD_MILLIS))
                                        .build())
                                .protocol(Protocol.HTTP2))
                .build();
    }

    @Bean
    @Profile("!test")
    public DynamoDbAsyncClient dynamoDbAsyncClient(AwsCredentialsProvider awsCredentialsProvider) {
        return DynamoDbAsyncClient.builder().credentialsProvider(awsCredentialsProvider)
                .region(Region.of(region)).build();
    }

    @Bean
    @Profile("!test")
    public CloudWatchAsyncClient cloudWatchAsyncClient(AwsCredentialsProvider awsCredentialsProvider) {
        return CloudWatchAsyncClient.builder().credentialsProvider(awsCredentialsProvider)
                .region(Region.of(region)).build();
    }
}
