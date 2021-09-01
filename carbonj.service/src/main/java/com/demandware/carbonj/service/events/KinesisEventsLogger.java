/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.RejectionHandler;
import com.demandware.carbonj.service.queue.InputQueue;
import com.demandware.carbonj.service.queue.QueueProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point to the events shipping functionality.   It holds the queue where all the events received from other
 * blades as well as the events from Carbonj are held.   Those events will be batched and compressed before they are
 * sent to a kinesis queue in metrics cloud.
 */
public class KinesisEventsLogger implements EventsLogger<byte[]> {

    private static final Logger log = LoggerFactory.getLogger( KinesisEventsLogger.class );

    private final InputQueue<byte[]> queue;

    KinesisEventsLogger(MetricRegistry metricRegistry, int queueSize, int emptyQueuePauseMillis, RejectionHandler<byte[]> rejectionHandler,
                        QueueProcessor<byte[]> queueProcessor, int batchSize, long maxWaitTimeMillis) {

        queue = new InputQueue<>(metricRegistry,"events", queueProcessor, queueSize, rejectionHandler, batchSize,
                emptyQueuePauseMillis, maxWaitTimeMillis);
        queue.start();
    }

    KinesisEventsLogger(MetricRegistry metricRegistry, String streamName, int queueSize, int emptyQueuePauseMillis, int noOfThreads, int batchSize, long maxWaitTimeMillis)
    {
            this(metricRegistry, queueSize, emptyQueuePauseMillis, new DropRejectionHandler<>(), new KinesisQueueProcessor(metricRegistry, streamName,
                    AmazonKinesisClientBuilder.defaultClient(), noOfThreads), batchSize, maxWaitTimeMillis);
    }

    @Override
    public void log(byte[] event)
    {
        queue.accept(event);
    }

    public void close() {
        log.info("Stopping kinesis events logger.");
        queue.close();
    }
}
