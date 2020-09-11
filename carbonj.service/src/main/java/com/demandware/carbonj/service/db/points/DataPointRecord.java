/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

class DataPointRecord
{
    public static byte[] toKeyBytes(long metricId, int ts)
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeLong( metricId );
        out.writeInt( ts );
        return out.toByteArray();
    }

    public static byte[] toValueBytes(double v)
    {
        return Longs.toByteArray(Double.doubleToLongBits(v));
    }

    public static double toValue(byte[] valueBytes)
    {
        return Double.longBitsToDouble( Longs.fromByteArray( valueBytes ) );
    }

    public static int toMetricId(byte[] keyBytes)
    {
        return Ints.fromBytes( keyBytes[0], keyBytes[1], keyBytes[2], keyBytes[3] );
    }

    public static int toTimestamp(byte[] keyBytes)
    {
        return Ints.fromBytes( keyBytes[4], keyBytes[5], keyBytes[6], keyBytes[7]);
    }
}
