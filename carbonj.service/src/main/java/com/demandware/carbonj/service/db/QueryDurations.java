/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db;

import java.util.concurrent.atomic.LongAdder;

/**
 * Accumulates time spent by all the tasks (batches) created for specific query.
 * The time spent is broken by operation - fetch, send, serialize.
 */
class QueryDurations
{
    final private LongAdder readTime = new LongAdder();
    final private LongAdder serializeAndSendTime = new LongAdder();


    QueryDurations()
    {
    }


    void addRead(long duration)
    {
        readTime.add(duration);
    }

    void addSerializeAndSend(long duration)
    {
        serializeAndSendTime.add(duration);
    }

    long read()
    {
        return readTime.longValue();
    }

    long serializeAndSend()
    {
        return serializeAndSendTime.longValue();
    }

}
