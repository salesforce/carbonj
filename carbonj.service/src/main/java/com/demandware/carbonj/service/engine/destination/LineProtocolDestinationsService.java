/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import com.salesforce.cc.infra.core.kinesis.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LineProtocolDestinationsService extends Destination implements LineProtocolDestination, ServiceDestination {

    private static final Logger logger = LoggerFactory.getLogger(LineProtocolDestinationsService.class);

    private LoadBalancedDestination loadBalancedDestination;
    private ScheduledExecutorService scheduledExecutorService;

    public LineProtocolDestinationsService(MetricRegistry metricRegistry, String type, String serviceName, int port, int queueSize, int batchSize, int refreshIntervalInMillis) {
        this(metricRegistry, type, serviceName, refreshIntervalInMillis, new LineProtocolDestinationFetcher(metricRegistry, type, serviceName, port, queueSize, batchSize));
    }

    public LineProtocolDestinationsService(MetricRegistry metricRegistry, String type, String serviceName, int refreshIntervalInMillis, DestinationFetcher destinationFetcher) {
        super(metricRegistry,"dest." + type + "." + serviceName);

        this.metricRegistry = metricRegistry;
        loadBalancedDestination = new LoadBalancedDestination(destinationFetcher.getCurrentDestinations());

        if (refreshIntervalInMillis > 0) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleAtFixedRate(new RefreshDestinationsTask(this, destinationFetcher), refreshIntervalInMillis, refreshIntervalInMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void closeQuietly() {
        loadBalancedDestination.closeQuietly();
        if (scheduledExecutorService != null) {
            try {
                scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                ; //ignored
            }
            scheduledExecutorService.shutdown();
        }
    }

    @Override
    public void accept(DataPoint dataPoint) {
        loadBalancedDestination.accept(dataPoint);
    }

    @Override
    public LineProtocolDestination[] getDestinations() {
        return loadBalancedDestination.getDestinations();
    }

    @Override
    public void setDestinations(LineProtocolDestination[] destinations) {
        LoadBalancedDestination previousDestination = loadBalancedDestination;
        loadBalancedDestination = new LoadBalancedDestination(destinations);
        previousDestination.closeQuietly();
    }

    static class RefreshDestinationsTask implements Runnable {

        private final ServiceDestination serviceDestination;
        private final DestinationFetcher destinationFetcher;

        RefreshDestinationsTask(ServiceDestination serviceDestination, DestinationFetcher destinationFetcher) {
            this.serviceDestination = serviceDestination;
            this.destinationFetcher = destinationFetcher;
        }

        @Override
        public void run() {
            LineProtocolDestination[] currentResults = destinationFetcher.getCurrentDestinations();
            if (currentResults != serviceDestination.getDestinations()) {
                serviceDestination.setDestinations(currentResults);
            }
        }
    }

    private static class LineProtocolDestinationFetcher implements DestinationFetcher {

        private  final MetricRegistry metricRegistry;
        private final String type;
        private final String serviceName;
        private final int port;
        private final int queueSize;
        private final int batchSize;
        private LineProtocolDestination[] lineDestinations;
        private Set<String> currentIpAddresses;


        private LineProtocolDestinationFetcher(MetricRegistry metricRegistry, String type, String serviceName, int port, int queueSize, int batchSize) {

            this.metricRegistry = metricRegistry;
            this.type = type;
            this.serviceName = serviceName;
            this.port = port;
            this.queueSize = queueSize;
            this.batchSize = batchSize;
        }

        private Set<String> getIpAddresses() {
            try {
                InetAddress dnsResults[] = InetAddress.getAllByName(serviceName);
                Set<String> currentResults = new HashSet<>();
                for (InetAddress dnsResult: dnsResults)  {
                    currentResults.add(dnsResult.getHostAddress());
                }
                return currentResults;
            } catch (Throwable e) {
                logger.error("Error getting dns results", e);
                return Collections.emptySet();
            }
        }

        private LineProtocolDestination[] toLineProtocolDestination(Set<String> ipAddresses) {
            LineProtocolDestination[] lineDestinations = new LineProtocolDestinationSocket[ipAddresses.size()];
            int i = 0;
            for (String currentResult: ipAddresses) {
                LineProtocolDestinationSocket newLineDestination = new LineProtocolDestinationSocket(metricRegistry,
                        type,  currentResult, port, queueSize, batchSize);
                lineDestinations[i++] = newLineDestination;
            }
            return lineDestinations;
        }

        @Override
        public LineProtocolDestination[] getCurrentDestinations() {
            Set<String> newIpAddresses = getIpAddresses();
            if (!newIpAddresses.equals(currentIpAddresses)) {
                currentIpAddresses = newIpAddresses;
                lineDestinations = toLineProtocolDestination(currentIpAddresses);
            }
            return lineDestinations;
        }
    }
}
