/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.google.common.primitives.Ints;

import static java.nio.charset.StandardCharsets.UTF_8;

class IdRecordSerializer
                implements RecordSerializer<Integer, IdRecord>
{
    public IdRecordSerializer()
    {
    }

    @Override
    public Integer key( byte[] keyBytes )
    {
        return Ints.fromByteArray( keyBytes );
    }

    @Override
    public IdRecord toIndexEntry( byte[] keyBytes, byte[] valueBytes)
    {
        Integer key = key(keyBytes);
        return toIndexEntry( key, valueBytes);
    }

    @Override
    public IdRecord toIndexEntry( Integer key, byte[] valueBytes)
    {
        String value = new String(valueBytes, UTF_8);
        return new IdRecord( key, value );
    }

    @Override
    public byte[] keyBytes(Integer key)
    {
        return Ints.toByteArray(key);
    }

    @Override
    public byte[] valueBytes(IdRecord e)
    {
        return e.metricName().getBytes( UTF_8 );
    }

}
