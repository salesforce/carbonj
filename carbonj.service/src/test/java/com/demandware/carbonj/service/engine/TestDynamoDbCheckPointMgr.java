/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest (webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class TestDynamoDbCheckPointMgr {

    private static final Logger logger = LoggerFactory.getLogger( TestDynamoDbCheckPointMgr.class );

    @Test
    @Ignore // ignoring because it is calling AWS API and it should not be
    public void testBasic() throws Exception {
        AmazonDynamoDB dynamoDbClient = AmazonDynamoDBClientBuilder.standard().build();
        CheckPointMgr<Date> checkPointMgr = new DynamoDbCheckPointMgr(dynamoDbClient, "test", 60, 1);
        Date lastCheckPoint = checkPointMgr.lastCheckPoint();
        Assert.assertTrue(lastCheckPoint.before(new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(60))));
        Date checkPoint1 = new Date(System.currentTimeMillis());
        checkPointMgr.checkPoint(checkPoint1);
        Assert.assertEquals(checkPoint1, checkPointMgr.lastCheckPoint());
        Date checkPoint2 = new Date();
        checkPointMgr.checkPoint(checkPoint2);
        Assert.assertEquals(checkPoint2, checkPointMgr.lastCheckPoint());
    }
}
