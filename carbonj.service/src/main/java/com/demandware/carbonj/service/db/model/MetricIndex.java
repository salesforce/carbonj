/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import com.demandware.carbonj.service.db.util.StatsAware;

/**
 * Index to access metric metadata.
 */
public interface MetricIndex
    extends MetricProvider, StatsAware
{
    void open();

    void close();

    /**
     * In strict mode exception is thrown when any data inconsistency is detected. And in non-strict mode details about
     * invalid state are logged and the data is skipped/ignored if possible. By default strictMode is set to false.
     * Primary use is for unit tests.
     */
    void setStrictMode( boolean mode );

    boolean isStrictMode();

    List<Metric> deleteMetric( String name, boolean recursive, boolean testRun );

    DeleteAPIResult deleteAPI( String name, boolean delete, Set<String> exclude );


//    /**
//     * Retrieves metrics that match provided pattern. Threshold based on configuration will be enforced.
//     * Invalid metric names will be skipped.
//     *
//     * @throws TooManyMetricsFoundException if threshold was exceeded.
//     * @return list of matches.
//     */
//    List<Metric> findLeafMetrics(String pattern) throws TooManyMetricsFoundException;

    /**
     * Retrieves metrics that match provided pattern without any threshold. Invalid metric names will be returned.
     */
    List<Metric> findMetrics( String pattern );

    /**
     * Retrieves metrics that match provided pattern.
     *
     * @throws TooManyMetricsFoundException if threshold was exceeded.
     * @return list of matches.
     */
    List<Metric> findMetrics( String pattern, boolean leafOnly, boolean useThreshold, boolean skipInvalid );

    /**
     * Returns all top-level metric names present in the index. Primarily will be used for tests and utilities that
     * check index consistency.
     */
    List<String> getTopLevelNames();

    /**
     * Returns all children names registered with provided metric name. Primarily will be used for tests and utilities
     * that check index consistency.
     *
     * @param name name of the metric for which we want to retrieve available metrics.
     * @return list of names attached to this metric name or empty list if there are none.
     */
    List<String> getChildNames( String name );

    Metric createLeafMetric( String name );

    Metric getMetric( String name );

    Metric getMetric( int metricId );

    boolean isValidName( String name );

    Metric selectRandomMetric();

    void dumpIndex( File dumpFile );

    int scanNames( int start, int end, Consumer<Metric> m );
}
