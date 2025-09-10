/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.net.URI;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.demandware.carbonj.service.engine.TestUtils.setEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

@Testcontainers
public class TestDynamoDbCheckPointMgr {

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4.7.0")).withServices(DYNAMODB);

    @Test
    public void testBasic() throws Exception {
        setEnvironmentVariable("AWS_ACCESS_KEY_ID", "accessKey");
        setEnvironmentVariable("AWS_SECRET_ACCESS_KEY", "secretKey");
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create(localstack.getEndpoint().toString()))
                .region(Region.US_EAST_1)
                .build();
        CheckPointMgr<Date> checkPointMgr = new DynamoDbCheckPointMgr(dynamoDbClient, "test", 60, 1);
        while (!DynamoDbUtils.isTablePresent(dynamoDbClient, "checkpoints-test")) {
            Thread.sleep(1000);
        }
        Date lastCheckPoint = checkPointMgr.lastCheckPoint();
        Thread.sleep(100);
        assertTrue(lastCheckPoint.before(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(60))));
        Date checkPoint1 = new Date(System.currentTimeMillis());
        checkPointMgr.checkPoint(checkPoint1);
        assertEquals(checkPoint1, checkPointMgr.lastCheckPoint());
        Date checkPoint2 = new Date();
        checkPointMgr.checkPoint(checkPoint2);
        assertEquals(checkPoint2, checkPointMgr.lastCheckPoint());
    }
}
