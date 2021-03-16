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
    public static byte[] toKeyBytes(long metricId, int ts, boolean longId)
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        if(longId)
        {
            out.writeLong( metricId );
        }
        else
        {
            out.writeInt((int)metricId);
        }

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

    public static Long toMetricId(byte[] keyBytes, boolean longId)
    {
        if(longId)
        {
            return Longs.fromBytes( keyBytes[0], keyBytes[1], keyBytes[2], keyBytes[3], keyBytes[4],
                    keyBytes[5], keyBytes[6], keyBytes[7]);
        }

        Integer metricId = Ints.fromBytes(keyBytes[0], keyBytes[1], keyBytes[2], keyBytes[3]);
        return metricId.longValue();
    }

    public static int toTimestamp(byte[] keyBytes, boolean longId)
    {
        if(longId)
        {
            return Ints.fromBytes( keyBytes[8], keyBytes[9], keyBytes[10], keyBytes[11]);
        }

        return Ints.fromBytes( keyBytes[4], keyBytes[5], keyBytes[6], keyBytes[7]);
    }
}
