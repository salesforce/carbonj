/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.kinesis.kcl;

import com.amazonaws.services.kinesis.leases.impl.Lease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class MemLeaseManager<T extends Lease> implements ILeaseManager<T> {

    private final long delayInMillis;

    private Map<String, T> shardIdToLease;

    public MemLeaseManager(long delayInMillis) {
        this.delayInMillis = delayInMillis;
    }

    @Override
    public boolean createLeaseTableIfNotExists(Long readCapacity, Long writeCapacity) {

        if (shardIdToLease == null) {
            shardIdToLease = new ConcurrentHashMap<>();
            return true;
        }
        return false;
    }

    @Override
    public boolean leaseTableExists() {
        return shardIdToLease != null;
    }

    @Override
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) {
        return shardIdToLease != null;
    }

    @Override
    public List<T> listLeases()  {
        return new ArrayList<>(shardIdToLease.values());
    }

    @Override
    public boolean createLeaseIfNotExists(T lease) {
        return shardIdToLease.putIfAbsent(lease.getLeaseKey(), lease) == null;
    }

    @Override
    public T getLease(String shardId) {
        return shardIdToLease.get(shardId);
    }

    @Override
    public boolean renewLease(T lease)  {
        lease.setLeaseCounter(lease.getLeaseCounter() + 1L);
        shardIdToLease.put(lease.getLeaseKey(), lease);
        return true;
    }

    @Override
    public boolean takeLease(T lease, String owner)  {

        // an hack to get around a KCL race condition bug
        try {
            Thread.sleep(delayInMillis);
        } catch (InterruptedException e) {
            ; //ignore
        }

        lease.setLeaseCounter(lease.getLeaseCounter() + 1L);
        lease.setLeaseOwner(owner);
        shardIdToLease.put(lease.getLeaseKey(), lease);
        return true;
    }

    @Override
    public boolean evictLease(T lease) {
        lease.setLeaseOwner((String)null);
        lease.setLeaseCounter(lease.getLeaseCounter() + 1L);
        shardIdToLease.put(lease.getLeaseKey(), lease);

        return true;
    }

    @Override
    public void deleteLease(T lease)  {
        shardIdToLease.remove(lease.getLeaseKey());
    }

    @Override
    public void deleteAll() {
        shardIdToLease.clear();
    }

    @Override
    public boolean updateLease(T lease) {
        shardIdToLease.put(lease.getLeaseKey(), lease);
        lease.setLeaseCounter(lease.getLeaseCounter() + 1L);
        return true;
    }

    @Override
    public boolean isLeaseTableEmpty() {
        return shardIdToLease.isEmpty();
    }
}
