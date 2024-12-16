/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest (webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Disabled
public class TestDynamoDbCheckPointMgr {

    @Test
    @Disabled // ignoring because it is calling AWS API and it should not be
    public void testBasic() throws Exception {
        DynamoDbAsyncClient dynamoDbClient = DynamoDbAsyncClient.builder().build();
        CheckPointMgr<Date> checkPointMgr = new DynamoDbCheckPointMgr(dynamoDbClient, "test", 60, 1, 30);
        Date lastCheckPoint = checkPointMgr.lastCheckPoint();
        assertTrue(lastCheckPoint.before(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(60))));
        Date checkPoint1 = new Date(System.currentTimeMillis());
        checkPointMgr.checkPoint(checkPoint1);
        assertEquals(checkPoint1, checkPointMgr.lastCheckPoint());
        Date checkPoint2 = new Date();
        checkPointMgr.checkPoint(checkPoint2);
        assertEquals(checkPoint2, checkPointMgr.lastCheckPoint());
    }
}
