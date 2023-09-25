/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.strings.StringsCache;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Immutable routing rules.
 */
public class RelayRules
{
    private static final Logger log = LoggerFactory.getLogger( RelayRules.class );

    final private File confFile;

    final private String confSrc;

    final private ConfigServerUtil configServerUtil;

    private List<String> configLines = new ArrayList<>();

    // rule order is important
    private LinkedHashMap<Pattern, String[]> rules = new LinkedHashMap<>();

    private boolean empty;

    private final String type;

    private final boolean cachingEnabled;

    private final String[] emptyResult = new String[0];
    /**
     * Creates instance with no rules and no destinations.
     */
    public RelayRules(String type)
    {
        this.type = type;
        this.confSrc = "file";
        this.configServerUtil = null;
        this.confFile = null;
        this.configLines = new ArrayList<>( );
        this.rules = new LinkedHashMap<>();
        this.empty = rules.isEmpty();
        this.cachingEnabled = "relay".equals(type);
    }

    /**
     * Initializes instance with rules loaded from file.
     *
     * @param confFile
     */
    public RelayRules( String type, File confFile, String confSrc, ConfigServerUtil configServerUtil )
    {
        this.type = type;
        this.confFile = Preconditions.checkNotNull( confFile );
        this.confSrc = Preconditions.checkNotNull(confSrc);
        this.configServerUtil = configServerUtil;
        log.debug( String.format( "Creating relay rules with config file [%s]", confFile ) );
        load();
        log.debug( "Relay rules created" );
        cachingEnabled = "relay".equals(type);
    }

    public boolean isEmpty()
    {
        return empty;
    }

    /**
     * Returns all configured destination groups.
     */
    public Set<String> allDestinationGroups()
    {
        Set<String> all = new HashSet<>();
        for ( String[] destinationGroups : rules.values() )
        {
            Collections.addAll(all, destinationGroups);
        }
        return all;
    }

    /**
     * All text representations for destination groups that this metric should be sent to.
     *
     * @param metricName metric name.
     *
     * @return
     */
    public String[] getDestinationGroups( String metricName )
    {
        return eval( metricName );
    }

    private String[] eval(String metricName)
    {
        if( empty )
        {
            return emptyResult;
        }

        // Support relay type only but not audit
        StringsCache.State state = cachingEnabled ? StringsCache.getState(metricName) : null;
        if (cachingEnabled) {
            if (state != null && state.getRelayDestinations() != null) {
                return state.getRelayDestinations();
            }
        }

        String[] relayDestination = emptyResult;
        // iterate over patterns and return the first match.
        for ( Map.Entry<Pattern, String[]> e : rules.entrySet() )
        {
            Pattern p = e.getKey();
            if( ".*".equals( p.pattern() ) )
            {
                relayDestination = e.getValue();
                break;
            }

            boolean m = p.matcher( metricName ).find();
            if ( m )
            {
                if ( log.isDebugEnabled() )
                {
                    log.debug( String.format( "Matched name [%s] on pattern [%s], Returning value [%s]", metricName, p,
                                    Arrays.toString(e.getValue()) ) );
                }
                relayDestination = e.getValue();
                break;
            }
            else if ( log.isDebugEnabled() ) {
                log.debug( String.format( "Name [%s] didn't match pattern [%s]", metricName, p ) );
            }
        }

        if( log.isDebugEnabled() )
        {
            log.debug( String.format( "Name [%s] didn't match any of the rules.", metricName ) );
        }

        if (state != null) {
            state.setRelayDestinations(relayDestination);
        }
        return relayDestination;
    }

    private void  load() {
        if (confSrc.equalsIgnoreCase("file")) {
            loadFromFile();
        } else if (confSrc.equalsIgnoreCase("server")) {
            if (!loadFromServer()) {
                log.error("Configuration couldn't be loaded from config server. Falling back to loading from file.");
                loadFromFile();
            }
        } else {
            throw new IllegalStateException("Unexpected configuration src: " + confSrc);
        }
    }

    private boolean loadFromServer() {
        if (configServerUtil == null) {
            log.debug("Relay rules config server undefined. Remove all destinations.");
            // removing relay config file results in disabling relay functionality.
            this.configLines = new ArrayList<>();
            this.rules = new LinkedHashMap<>();
            return false;
        }
        Optional<List<String>> configLines = configServerUtil.getConfigLines(type + "-rules");
        if (configLines.isPresent()) {
            this.configLines = configLines.get();
        } else {
            log.error("Failed to load configuration from config server for type: {}.", type);
            return false;
        }
        this.rules = parseConfig(this.configLines);
        this.empty = this.rules.isEmpty();
        if (log.isDebugEnabled()) {
            log.debug("Loaded rules from config server type {}, rules {}", type, rules);
        }
        return true;
    }

    private void loadFromFile()
    {
        if ( !confFile.exists() )
        {
            log.debug( String.format( "Relay rules configuration file doesn't exist. Remove all destinations. File: [%s]", confFile ) );
            // removing relay config file results in disabling relay functionality.
            this.configLines = new ArrayList<>();
            this.rules = new LinkedHashMap<>();
            return;
        }

        this.configLines = loadRulesFromFile();
        this.rules = parseConfig( configLines );
        this.empty = this.rules.isEmpty();
        if ( log.isDebugEnabled() )
        {
            log.debug( String.format( "Loaded relay rules %s", rules ) );
        }
    }

    private LinkedHashMap<Pattern, String[]> parseConfig( List<String> lines )
    {
        LinkedHashMap<Pattern, String[]> rules = new LinkedHashMap<>();

        for ( String line : lines )
        {
            int i = line.lastIndexOf( "=" );
            if ( i <= 0 )
            {
                log.warn( String.format( "Skipping invalid line in relay rules config. Line [%s], file [%s]", line,
                                confFile ) );
            }

            String regEx = line.substring( 0, i );
            String destinations = line.substring( i + 1 );

            Pattern pattern = Pattern.compile( regEx );
            rules.put( pattern, destinations.split( "\\|" ) );
        }

        return rules;
    }

    /**
     * Load lines from file. Trims whitespace, removes comments and empty lines.
     */
    private List<String> loadRulesFromFile()
    {
        try
        {
            return FileUtils.readLines( confFile ).stream()
                            .map(String::trim)
                            .filter( line -> line.length() != 0 && !line.startsWith( "#" ) )
                            .collect( Collectors.toList() );
        }
        catch(IOException e)
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !(o instanceof RelayRules that) )
            return false;

        return configLines.equals( that.configLines );

    }

    @Override
    public int hashCode()
    {
        return configLines.hashCode();
    }

    @Override
    public String toString()
    {
        return "RelayRules{" +
                        "configLines=" + configLines +
                        '}';
    }
}
