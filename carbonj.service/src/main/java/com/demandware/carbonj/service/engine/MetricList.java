/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.util.StatsAware;
import com.demandware.carbonj.service.strings.StringsCache;
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

    volatile private List<Pattern> patterns = new ArrayList<>(  );

    private final String confSrc;

    private final ConfigServerUtil configServerUtil;

    public MetricList(MetricRegistry metricRegistry,  String name, File confFile, String confSrc,
                     ConfigServerUtil configServerUtil )
    {
        this.name = Preconditions.checkNotNull(name);
        this.confFile = Preconditions.checkNotNull( confFile );
        log.info( String.format("Creating metric list [%s] with config file [%s]", name, confFile) );
        this.droppedMetrics = metricRegistry.counter( MetricRegistry.name( name, "drop" ) );
        this.confSrc = confSrc;
        this.configServerUtil = configServerUtil;
        reload();
        log.info(String.format("Metric list [%s] created.", name) );
    }

    public boolean match(String name)
    {
        if ( patterns.isEmpty() )
        {
            return false;
        }

        StringsCache.State state = StringsCache.getState(name);
        if (state != null && state.getBlackListed() != null) {
            return state.getBlackListed();
        }

        List<Pattern> currentPatterns = patterns; // copy so we don't keep hitting the volatile barrier
        boolean isBlackListed = false;
        for ( Pattern p : currentPatterns )
        {
            if( ".*".equals( p.pattern() ) )
            {
                droppedMetrics.inc();
                isBlackListed = true;
                break;
            }

            if ( p.matcher( name ).find() )
            {
                droppedMetrics.inc();
                isBlackListed = true;
                break;
            }
        }
        if (state != null) {
            state.setBlackListed(isBlackListed);
        }
        return isBlackListed;
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
                if (configServerUtil == null || configServerUtil.getConfigLines(name).isEmpty()) {
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

            this.patterns = parseConfig( lines );
            this.configLines = lines;
            StringsCache.invalidateCache();
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

    private List<Pattern> parseConfig(List<String> lines)
    {
        return lines.stream()
                    .map( String::trim )
                    .filter( line -> line.length() > 0 && !line.startsWith( "#" ) )
                    .map( Pattern::compile ).collect( Collectors.toList() );
    }

    @Override
    public void dumpStats()
    {
        log.info( String.format( "metric list [%s] counter=%s", name, droppedMetrics.getCount()) );
    }

    public boolean isEmpty()
    {
        return patterns.isEmpty();
    }
}
