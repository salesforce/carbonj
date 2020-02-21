/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.kcl;

import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.demandware.carbonj.service.engine.kinesis.kcl.MemLeaseManager;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class TestMemLeaseManager {

    private static final String OWNER = "owner";
    private static final int NO_OF_SHARDS = 5;

    @Test
    public void testBasicPath() throws Exception {
        ILeaseManager<KinesisClientLease> leaseManager = new MemLeaseManager<>(1);

        // check if the lease table exists
        Assert.assertFalse(leaseManager.leaseTableExists());

        // create table if it not exists
        Assert.assertTrue(leaseManager.createLeaseTableIfNotExists(10L, 10L));

        // check if the lease table exists
        Assert.assertTrue(leaseManager.waitUntilLeaseTableExists(1, 10));

        // check if the lease table exists
        Assert.assertTrue(leaseManager.leaseTableExists());

        // check if the lease table is empty
        Assert.assertTrue(leaseManager.isLeaseTableEmpty());

        Assert.assertEquals(0, leaseManager.listLeases().size());

        // create lease for 5 shards
        for (int i = 0; i < NO_OF_SHARDS; i++) {
            String shardId = "shard-" + i;
            KinesisClientLease lease = newKCLLease(shardId);
            Assert.assertTrue(leaseManager.createLeaseIfNotExists(lease));
        }

        // check if the lease table is not empty
        Assert.assertFalse(leaseManager.isLeaseTableEmpty());

        Assert.assertEquals(5, leaseManager.listLeases().size());

        verify(leaseManager, null, 0L);

        for (int i = 0; i < NO_OF_SHARDS; i++) {
            String shardId = "shard-" + i;
            KinesisClientLease lease = leaseManager.getLease(shardId);
            leaseManager.takeLease(lease, OWNER);
        }

        verify(leaseManager, OWNER, 1L);

        renewLeases(leaseManager);

        verify(leaseManager, OWNER, 2L);

        updateLeases(leaseManager);

        verify(leaseManager, OWNER, 3L);
    }

    private void updateLeases(ILeaseManager<KinesisClientLease> leaseManager) throws Exception {
        for (int i = 0; i < NO_OF_SHARDS; i++) {
            String shardId = "shard-" + i;
            KinesisClientLease lease = leaseManager.getLease(shardId);
            leaseManager.updateLease(lease);
        }
    }

    private void verify(ILeaseManager<KinesisClientLease> leaseManager, String expectedOwner, Long expectedLeaseCounter) throws Exception {
        for (int i = 0; i < NO_OF_SHARDS; i++) {
            String shardId = "shard-" + i;
            KinesisClientLease lease = leaseManager.getLease(shardId);
            Assert.assertEquals(expectedOwner, lease.getLeaseOwner());
            Assert.assertEquals(expectedLeaseCounter, lease.getLeaseCounter());
        }
        Assert.assertEquals(NO_OF_SHARDS, leaseManager.listLeases().size());
    }

    private static KinesisClientLease newKCLLease(String shardId) {
        KinesisClientLease newLease = new KinesisClientLease();
        newLease.setLeaseKey(shardId);
        List<String> parentShardIds = new ArrayList<>();
        newLease.setParentShardIds(parentShardIds);
        newLease.setOwnerSwitchesSinceCheckpoint(0L);

        return newLease;
    }

    private void renewLeases(ILeaseManager<KinesisClientLease> leaseManager) throws Exception {
        for (int i = 0; i < NO_OF_SHARDS; i++) {
            String shardId = "shard-" + i;
            KinesisClientLease lease = leaseManager.getLease(shardId);
            leaseManager.renewLease(lease);
        }
    }
}
