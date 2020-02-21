/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import java.util.Objects;

public class ShardInfoImpl implements ShardInfo {

    private String shardId;

    public ShardInfoImpl(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public String getShardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardInfoImpl shardInfo = (ShardInfoImpl) o;
        return Objects.equals(shardId, shardInfo.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId);
    }

    @Override
    public String toString() {
        return shardId;
    }
}
