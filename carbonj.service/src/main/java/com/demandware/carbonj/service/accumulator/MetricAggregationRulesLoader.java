/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.demandware.carbonj.service.strings.StringsCache;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demandware.carbonj.service.config.ConfigUtils;

/**
 * Monitors configuration file and loads metric aggregation rules when the file changes.
 */
class MetricAggregationRulesLoader
{
    private static final Logger log = LoggerFactory.getLogger( MetricAggregationRulesLoader.class );

    final private File confFile;

    private List<String> lines;

    volatile private MetricAggregationRules rules;

    private final boolean aggregationRuleCacheEnabled;

    public MetricAggregationRulesLoader( File confFile, boolean aggregationRuleCacheEnabled )
    {
        this.aggregationRuleCacheEnabled = aggregationRuleCacheEnabled;
        this.confFile = Preconditions.checkNotNull( confFile );
        this.lines = Collections.emptyList();
        this.rules = new MetricAggregationRules( 0, Collections.emptyList() );
        log.info( String.format( "Creating metric aggregation rules with config file [%s]", confFile ) );
        reload();
        log.info( "metric aggregation rules created" );
    }


    public void reload()
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "Check for metric aggregation rule configuration update" );
        }

        try
        {
            if( !confFile.exists() )
            {
                log.warn( String.format("Configuration file with metric aggregation rules doesn't exist. File: [%s]", confFile) );

                if( rules.size() > 0 )
                {
                    log.warn( "Clear current metric aggregation rules." );
                    int revision = rules.getRevision();
                    this.rules = new MetricAggregationRules( revision, Collections.emptyList() );
                }
                return;
            }

            List<String> newLines = ConfigUtils.lines( confFile );

            if( !lines.equals( newLines ) )
            {
                // rules changed
                log.info(String.format("metric aggregation rules configuration file has changed. File: [%s]", confFile));
                int nextRevision = rules.getRevision() + 1;
                this.rules = new MetricAggregationRules( nextRevision, parseConfig( newLines ) );
                this.lines = newLines;
                log.info( String.format( "New metric aggregation rules %s", lines ) );
                StringsCache.invalidateCache();
            }
        }
        catch ( Exception e )
        {
            log.error( "Failed to reload metric aggregation rules. Suppress. ", e );
        }
    }

    public MetricAggregationRules getRules()
    {
        return rules;
    }

    private List<MetricAggregationRule> parseConfig( List<String> lines )
    {
        List<MetricAggregationRule> metricAggregationRules = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            metricAggregationRules.add(
                    MetricAggregationRule.parseDefinition(lines.get(i), i, aggregationRuleCacheEnabled));
        }
        return metricAggregationRules;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper( this )
                      .add( "confFile", confFile )
                      .toString();
    }
}
