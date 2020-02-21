/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.RejectionHandler;
import com.demandware.carbonj.service.queue.InputQueue;
import com.demandware.carbonj.service.queue.QueueProcessor;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.net.InetAddress;

public class CarbonJEventsLogger implements EventsLogger<CarbonjEvent> {

    private final Gson gson;
    private final InputQueue<JsonObject> queue;
    private final String podName;
    private final String namespace;

    CarbonJEventsLogger( MetricRegistry metricRegistry, int batchSize, int emptyQueuePauseMillis, RejectionHandler<JsonObject> rejectionHandler,
                        QueueProcessor<JsonObject> queueProcessor, String hostname, long maxWaitTimeMillis)
    {
        gson = new Gson();

        // carbonj-p1-0.carbonj-p1.carbonj-a.svc.cluster.local
        String[] splits = hostname.split("\\.");
        if (splits.length == 6) {
            podName = splits[0];
            namespace = splits[2];
        } else {
            podName = namespace = hostname;
        }

        queue = new InputQueue<>(metricRegistry, "cjevents", queueProcessor, batchSize * 2, rejectionHandler,
                batchSize, emptyQueuePauseMillis, maxWaitTimeMillis);
        queue.start();
    }

    CarbonJEventsLogger(MetricRegistry metricRegistry, EventsLogger<byte[]> downstreamEventsLogger, int batchSize, int emptyQueuePauseMillis,
                        long maxWaitTimeMillis) throws Exception
    {
            this( metricRegistry, batchSize, emptyQueuePauseMillis, new DropRejectionHandler<>(),
                    new CarbonjEventsQueueProcessor(metricRegistry, downstreamEventsLogger), InetAddress.getLocalHost().getHostName(),
                    maxWaitTimeMillis);
    }

    @Override
    public void log(CarbonjEvent event) {
        JsonObject json = gson.toJsonTree(event).getAsJsonObject();
        json.addProperty("pod", podName);
        json.addProperty("namespace", namespace);
        queue.accept(json);
    }
}
