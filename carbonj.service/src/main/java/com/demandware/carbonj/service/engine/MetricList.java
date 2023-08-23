/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.apache.commons.lang3.tuple.Pair;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.re2j.Pattern;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * List of metrics that should receive special treatment (used for blocking or allowing metric names).
 */
public class MetricList implements StatsAware
{
    private static final Logger log = LoggerFactory.getLogger( MetricList.class );

    final private Counter droppedMetrics;

    final private File confFile;

    final private String name;

    volatile private List<String> configLines = new ArrayList<>(  );

    volatile private List<Pair<Pattern, Counter>> patternsAndCounters = new ArrayList<>(  );

    volatile private boolean empty = patternsAndCounters.isEmpty();

    private final String confSrc;

    private final ConfigServerUtil configServerUtil;

    private final MetricRegistry metricRegistry;

    public MetricList(MetricRegistry metricRegistry,  String name, File confFile, String confSrc,
                      ConfigServerUtil configServerUtil )
    {
        this.name = Preconditions.checkNotNull(name);
        this.confFile = Preconditions.checkNotNull( confFile );
        log.info( String.format("Creating metric list [%s] with config file [%s]", name, confFile) );
        this.metricRegistry = metricRegistry;
        this.droppedMetrics = metricRegistry.counter( MetricRegistry.name( name, "drop" ) );
        this.confSrc = confSrc;
        this.configServerUtil = configServerUtil;
        reload();
        log.info(String.format("Metric list [%s] created.", name) );
    }

    public boolean match(String name)
    {
        if ( patternsAndCounters.isEmpty() )
        {
            return false;
        }

        List<Pair<Pattern, Counter>> currentPatternsAndCounters = patternsAndCounters; // copy so we don't keep hitting the volatile barrier
        for ( int i = 0; i < currentPatternsAndCounters.size(); i++ )
        {
            Pair<Pattern, Counter> p = currentPatternsAndCounters.get(i);

            if( ".*".equals( p.getLeft().pattern() ) )
            {
                patternsAndCounters.get(i).getRight().inc();
                droppedMetrics.inc();
                return true;
            }

            long startTime = 0;
            if (log.isDebugEnabled()) {
                startTime = System.nanoTime(); // Record start time
            }
            if ( p.getLeft().matcher( name ).find() )
            {
                patternsAndCounters.get(i).getRight().inc();
                droppedMetrics.inc();
                if (log.isDebugEnabled()) {
                    long endTime = System.nanoTime();   // Record end time
                    long duration = endTime - startTime; // Calculate duration in nanoseconds
                    log.debug("Pattern match runtime for {}: {} nanoseconds", p.getLeft().pattern(), duration);
                }
                return true;
            }

        }
        return false;
    }

    public void reload()
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "Check for configuration update" );
        }

        try
        {
            List<String> lines;
            if (confSrc.equalsIgnoreCase("file")) {
                if (!confFile.exists()) {
                    log.warn(String.format("Metric list [%s] configuration file doesn't exist. File: [%s]", name, confFile));
                    return;
                }
                lines = FileUtils.readLines(confFile, Charsets.UTF_8);
            } else if (confSrc.equalsIgnoreCase("server")) {
                if (configServerUtil == null || !configServerUtil.getConfigLines(name).isPresent()) {
                    log.warn("Unable to read metric list configuration from config server. Falling back to file.");
                    if (!confFile.exists()) {
                        log.warn(String.format("Metric list [%s] configuration file doesn't exist. File: [%s]", name, confFile));
                        return;
                    }
                    lines = FileUtils.readLines(confFile, Charsets.UTF_8);
                } else {
                    lines = configServerUtil.getConfigLines(name).get();
                }
            } else {
                throw new RuntimeException("Unknown metric list config src: " + confSrc);
            }

            if( configLines.equals( lines ) )
            {
                // no config change
                return;
            }

            log.info(String.format("Metric list [%s] configuration file has changed. File: [%s]", name, confFile));

            List<String> oldLines = this.configLines;
            List<Pair<Pattern, Counter>> newPatternsAndCounters = parseConfig( lines );
            this.patternsAndCounters = newPatternsAndCounters;
            this.configLines = lines;
            log.info(String.format("Metric list [%s] updated.", name));
            if( log.isDebugEnabled() )
            {
                log.debug( String.format( "Metric list [%s] previous: %s, new %s", name, oldLines, lines ) );
            }
        }
        catch ( Exception e )
        {
            log.error(String.format("Failed to reload metric list [%s] config. Suppress. ", name), e );
        }
    }

    private List<Pair<Pattern, Counter>> parseConfig(List<String> lines)
    {
        // Create an empty list to hold pairs of Pattern and Counter
        List<Pair<Pattern, Counter>> patternCounterPairs = lines.stream()
                .map(String::trim)
                .filter(line -> line.length() > 0 && !line.startsWith("#"))
                .map(line -> {
                    Pattern pattern = Pattern.compile(line);
                    Counter counter = metricRegistry.counter( MetricRegistry.name( name, "blacklist" ) );
                    return Pair.of(pattern, counter); // Create and return the Pair
                })
                .collect(Collectors.toList());

        // Reset the counters here if needed
        for (Pair<Pattern, Counter> pair : patternCounterPairs) {
            pair.getValue().dec(pair.getValue().getCount()); // Reset the counter to zero
        }
        return patternCounterPairs;
    }

    @Override
    public void dumpStats()
    {
        log.info( String.format( "metric list [%s] counter=%s", name, droppedMetrics.getCount()) );
    }

    public boolean isEmpty()
    {
        return patternsAndCounters.isEmpty();
    }
}