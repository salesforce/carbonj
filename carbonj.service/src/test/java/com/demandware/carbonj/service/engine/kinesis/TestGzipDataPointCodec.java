/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.kinesis;

import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGzipDataPointCodec {
    @Test
    public void testDefault() {
        int current = (int) (System.currentTimeMillis() / 1000);
        DataPoint dataPoint = new DataPoint("foo.bar", 123, current);
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(dataPoint);
        DataPoints codecDataPoints = new DataPoints(dataPoints, current);
        GzipDataPointCodec codec = new GzipDataPointCodec();
        byte[] encoded = codec.encode(codecDataPoints);
        DataPoints decodedDataPoints = codec.decode(encoded);
        assertEquals(codecDataPoints.getTimeStamp(), decodedDataPoints.getTimeStamp());
        assertEquals(codecDataPoints.getDataPoints().size(), decodedDataPoints.getDataPoints().size());
        assertEquals(codecDataPoints.getDataPoints().get(0), decodedDataPoints.getDataPoints().get(0));
    }
}
