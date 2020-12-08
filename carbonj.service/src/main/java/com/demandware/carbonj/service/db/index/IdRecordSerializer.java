/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import static java.nio.charset.StandardCharsets.UTF_8;

class IdRecordSerializer
                implements RecordSerializer<Long, IdRecord>
{
    private boolean longId;

    public IdRecordSerializer(boolean longId)
    {
        this.longId = longId;
    }

    @Override
    public Long key( byte[] keyBytes )
    {
        return longId ? Longs.fromByteArray( keyBytes ) : Integer.valueOf(Ints.fromByteArray(keyBytes)).longValue();
    }

    @Override
    public IdRecord toIndexEntry( byte[] keyBytes, byte[] valueBytes)
    {
        Long key = key(keyBytes);
        return toIndexEntry( key, valueBytes);
    }

    @Override
    public IdRecord toIndexEntry( Long key, byte[] valueBytes)
    {
        String value = new String(valueBytes, UTF_8);
        return new IdRecord( key, value );
    }

    @Override
    public byte[] keyBytes(Long key)
    {
        return longId ? Longs.toByteArray(key) : Ints.toByteArray(key.intValue());
    }

    @Override
    public byte[] valueBytes(IdRecord e)
    {
        return e.metricName().getBytes( UTF_8 );
    }

}
