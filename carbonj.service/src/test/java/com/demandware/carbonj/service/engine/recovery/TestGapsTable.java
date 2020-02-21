/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestGapsTable {
    private static final Logger log = LoggerFactory.getLogger(TestGapsTable.class);

    @Test
    public void testCRUD_File() throws Exception {
        try {
            GapsTable gapsTable = new FileSystemGapsTableImpl(Paths.get("/tmp"));
            testIt(gapsTable);
            Assert.assertTrue(Files.notExists(Paths.get("/", "tmp", "gaps.txt")));
        } finally {
            //client.shutdown();
        }
    }

    @Test
    @Ignore // ignoring because it is calling AWS API and it should not be
    public void testCRUD_Dyn() throws Exception {
        GapsTable dynGapsTable = new DynamoDbGapsTableImpl(AmazonDynamoDBClientBuilder.standard().build(), "testApplication", 2);
        testIt(dynGapsTable);
    }

    private void testIt(GapsTable gapsTable) throws Exception {
        Date now = new Date();
        long nowTime = now.getTime();
        Date time3 = new Date(nowTime - TimeUnit.HOURS.toMillis(2));
        Date time2 = new Date(nowTime - TimeUnit.HOURS.toMillis(3));
        Date time1 = new Date(nowTime - TimeUnit.HOURS.toMillis(4));

        Gap firstGap = new GapImpl(time1, time2);
        Gap latestGap = new GapImpl(time3, now);

        gapsTable.add(firstGap);
        gapsTable.add(latestGap);

        List<Gap> gaps = gapsTable.getGaps();
        Assert.assertEquals(2, gaps.size());

        Assert.assertEquals(firstGap, gaps.get(0));
        Assert.assertEquals(latestGap, gaps.get(1));

        gapsTable.delete(firstGap);

        gaps = gapsTable.getGaps();
        Assert.assertEquals(1, gaps.size());
        Assert.assertEquals(latestGap, gaps.get(0));

        Assert.assertEquals(latestGap.startTime(), latestGap.lastRecovered());
        Date lastRecovered = new Date(nowTime + TimeUnit.HOURS.toMillis(1));
        Gap updatedGap = new GapImpl(latestGap.startTime(), latestGap.endTime(), lastRecovered);
        gapsTable.updateGap(updatedGap);

        // verify updated gap
        gaps = gapsTable.getGaps();
        Assert.assertEquals(1, gaps.size());
        Gap newlyFetchedGap = gaps.get(0);
        Assert.assertEquals(updatedGap.startTime(), newlyFetchedGap.startTime());
        Assert.assertEquals(updatedGap.endTime(), newlyFetchedGap.endTime());
        Assert.assertEquals(lastRecovered, newlyFetchedGap.lastRecovered());

        gapsTable.delete(newlyFetchedGap);

        gaps = gapsTable.getGaps();
        Assert.assertEquals(0, gaps.size());

        gapsTable.destroy();
    }
}
