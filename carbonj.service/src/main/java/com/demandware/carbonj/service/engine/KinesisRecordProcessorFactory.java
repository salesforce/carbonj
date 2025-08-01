/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;


import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;

import java.util.Date;


public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    private final MetricRegistry metricRegistry;

    public static Meter metricsReceived;

    public static Meter messagesReceived;

    private static Histogram pointsPerTask;

    private static Meter dropped;

    private static Meter messagesRetry;

    private static Meter taskCount;

    private static Timer consumerTimer;

    private static Histogram latency;

    private final PointProcessor pointProcessor;
    private final KinesisConfig kinesisConfig;
    private final String streamName;
    private final DataPointCodec codec;
    private final CheckPointMgr<Date> checkPointMgr;


    KinesisRecordProcessorFactory(MetricRegistry metricRegistry, PointProcessor pointProcessor, KinesisConfig kinesisConfig, String streamName, CheckPointMgr<Date> checkPointMgr) {
        this.metricRegistry = metricRegistry;
        this.pointProcessor = pointProcessor;
        this.kinesisConfig = kinesisConfig;
        this.streamName = streamName;
        this.checkPointMgr = checkPointMgr;

        metricsReceived = metricRegistry.meter(MetricRegistry.name("kinesis", "metricsRecieved"));

        messagesReceived = metricRegistry.meter(MetricRegistry.name("kinesis", "messagesRecieved"));

        pointsPerTask = metricRegistry.histogram(
                MetricRegistry.name("kinesis", "pointsPerTask"));

        dropped = metricRegistry.meter(MetricRegistry.name("kinesis", "dropped"));

        messagesRetry = metricRegistry.meter(MetricRegistry.name("kinesis","getRetry"));

        taskCount = metricRegistry.meter(MetricRegistry.name( "kinesis", "taskCount" ) );

        consumerTimer = metricRegistry.timer(MetricRegistry.name( "kinesis", "consumerTimer" ) );

        latency = metricRegistry.histogram(MetricRegistry.name( "kinesis", "latency" ) );
        codec = new GzipDataPointCodec();
    }

    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(metricRegistry, pointProcessor, metricsReceived, messagesReceived, pointsPerTask,
                kinesisConfig, messagesRetry, dropped, taskCount, consumerTimer, latency, codec, streamName, checkPointMgr);
    }
}
