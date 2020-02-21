/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.kinesis;

import com.demandware.carbonj.service.engine.DataPoint;

import java.util.Collections;
import java.util.List;

public class DataPoints {

    public static final DataPoints EMPTY = new DataPoints(Collections.EMPTY_LIST, -1);

    private List<DataPoint> dataPoints;
    private long timeStamp;

    public DataPoints(List<DataPoint> dataPoints, long timeStamp) {
        this.dataPoints = dataPoints;
        this.timeStamp = timeStamp;
    }

    public List<DataPoint> getDataPoints() {
        return dataPoints;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
}
