/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.config.ConfigUtils;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Monitors configuration file and loads storage aggregation rules when the file changes.
 */
public class StorageAggregationRulesLoader
{
    private static final Logger log = LoggerFactory.getLogger( StorageAggregationRulesLoader.class );

    final private File confFile;

    private List<String> lines = Collections.EMPTY_LIST;

    volatile private StorageAggregationRules rules;

    public StorageAggregationRulesLoader( File confFile )
    {
        this.confFile = Preconditions.checkNotNull( confFile );
        this.lines = Collections.EMPTY_LIST;
        this.rules = new StorageAggregationRules( 0, Collections.EMPTY_LIST );
        log.info( String.format( "Creating storage aggregation rules with config file [%s]", confFile ) );
        reload();
        log.info( "Storage aggregation rules created" );
    }


    // synchronized to make sure that there is only one reload running at a time
    public synchronized void reload()
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( "Check for storage aggregation rule configuration update" );
        }

        try
        {
            if( !confFile.exists() )
            {
                log.warn( String.format("Configuration file with storage aggregation rules doesn't exist. File: [%s]", confFile) );

                if( rules.size() > 0 )
                {
                    log.warn( "Clear current storage aggregation rules." );
                    this.lines = Collections.EMPTY_LIST;
                    this.rules = new StorageAggregationRules( rules.getRevision() + 1, Collections.EMPTY_LIST );
                }

                return;
            }

            List<String> newLines = ConfigUtils.lines(confFile);

            if( !newLines.equals( lines ) )
            {
                log.info(String.format("Storage aggregation rules configuration file has changed. File: [%s]", confFile));
                this.rules = new StorageAggregationRules( rules.getRevision() + 1, parseConfig( newLines ) );
                this.lines = newLines;
                log.info( String.format( "New storage aggregation rules %s", lines ) );
            }

        }
        catch ( Exception e )
        {
            log.error( "Failed to reload storage aggregation rules. Suppress. ", e );
        }
    }

    public StorageAggregationRules getRules()
    {
        return rules;
    }

    private List<StorageAggregationRule> parseConfig( List<String> lines )
    {
        return lines.stream()
                    .map( line -> StorageAggregationRule.parseDefinition( line )  )
                    .collect( Collectors.toList());
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper( this )
                      .add( "confFile", confFile )
                      .toString();
    }
}
