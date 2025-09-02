/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGapsTable {

    @Test
    public void testCRUD_File() throws Exception {
        GapsTable gapsTable = new FileSystemGapsTableImpl(Paths.get("/tmp"));
        testIt(gapsTable);
        assertTrue(Files.notExists(Paths.get("/", "tmp", "gaps.txt")));
    }

    @Test
    @Disabled // ignoring because it is calling AWS API and it should not be
    public void testCRUD_Dyn() throws Exception {
        GapsTable dynGapsTable = new DynamoDbGapsTableImpl(DynamoDbClient.builder().build(), "testApplication", 2);
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
        assertEquals(2, gaps.size());

        assertEquals(firstGap, gaps.get(0));
        assertEquals(latestGap, gaps.get(1));

        gapsTable.delete(firstGap);

        gaps = gapsTable.getGaps();
        assertEquals(1, gaps.size());
        assertEquals(latestGap, gaps.get(0));

        assertEquals(latestGap.startTime(), latestGap.lastRecovered());
        Date lastRecovered = new Date(nowTime + TimeUnit.HOURS.toMillis(1));
        Gap updatedGap = new GapImpl(latestGap.startTime(), latestGap.endTime(), lastRecovered);
        gapsTable.updateGap(updatedGap);

        // verify updated gap
        gaps = gapsTable.getGaps();
        assertEquals(1, gaps.size());
        Gap newlyFetchedGap = gaps.get(0);
        assertEquals(updatedGap.startTime(), newlyFetchedGap.startTime());
        assertEquals(updatedGap.endTime(), newlyFetchedGap.endTime());
        assertEquals(lastRecovered, newlyFetchedGap.lastRecovered());

        gapsTable.delete(newlyFetchedGap);

        gaps = gapsTable.getGaps();
        assertEquals(0, gaps.size());

        gapsTable.destroy();
    }
}
