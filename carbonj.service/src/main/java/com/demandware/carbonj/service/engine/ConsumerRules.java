/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ConsumerRules {

    private File rulesFile;

    private static Logger log = LoggerFactory.getLogger( ConsumerRules.class );

    private Set<String> configLines;

    ConsumerRules(File rulesFile) {

        this.rulesFile = Preconditions.checkNotNull( rulesFile );
        log.debug( String.format( " Creating consumer rules with config file [%s] ", rulesFile ) );
        configLines = new HashSet<>();
    }

    public Set<String> load()
    {
        if ( !rulesFile.exists() )
        {
            log.debug( String.format( "Consumer rules configuration file doesn't exist. Remove all consumers. File: [%s]", rulesFile ) );
            configLines = new HashSet<>();
            return configLines;
        }
        loadRulesFromFile();
        if ( log.isDebugEnabled() )
        {
            log.debug( String.format( "Loaded consumer config. rules %s", rulesFile ) );
        }
        return configLines;
    }

    /**
     * Load lines from file. Trims whitespace, removes comments and empty lines.
     */
    private void loadRulesFromFile()
    {
        try
        {
            configLines =  FileUtils.readLines( rulesFile ).stream()
                    .map( line -> line.trim() )
                    .filter( line -> line.length() != 0 && !line.startsWith( "#" ) )
                    .collect( Collectors.toSet() );
        }
        catch(IOException e)
        {
            throw new UncheckedIOException( e );
        }
    }

    public Set<String> getCurrentRules()
    {
        return configLines;
    }

    public void putCurrentRules(Set<String> currentRules) {
        this.configLines = currentRules;
    }
}
