/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.strings.StringsCache;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class Relay
    implements Consumer<DataPoints>
{
    private static final Logger log = LoggerFactory.getLogger( Relay.class );

    private final MetricRegistry metricRegistry;

    private final String type;

    private final int refreshIntervalInMillis;

    private final String destConfigDir;
    private final int maxWaitTimeInMillis;

    private final File rulesFile;

    private final ConfigServerUtil configServerUtil;

    private final String rulesSrc;

    private final int queueSize;

    private final int batchSize;

    volatile RelayRouter router;

    private final String kinesisRelayRegion;

    private final boolean relayCacheEnabled;

    Relay(MetricRegistry metricRegistry, String type, File rulesFile, int queueSize, int batchSize, int refreshIntervalInMillis, String destConfigDir,
          int maxWaitTimeInMillis, String kinesisRelayRegion, String relayRulesSrc, boolean relayCacheEnabled, ConfigServerUtil configServerUtil )
    {
        this.metricRegistry = metricRegistry;
        this.type = type;
        this.refreshIntervalInMillis = refreshIntervalInMillis;
        this.destConfigDir = destConfigDir;
        this.maxWaitTimeInMillis = maxWaitTimeInMillis;
        log.info( String.format("[%s] Starting relay", type) );
        this.rulesSrc = Preconditions.checkNotNull(relayRulesSrc);
        this.relayCacheEnabled = relayCacheEnabled;
        this.configServerUtil = configServerUtil;
        this.rulesFile = Preconditions.checkNotNull( rulesFile );
        this.queueSize = queueSize;
        this.batchSize = batchSize;
        this.kinesisRelayRegion = kinesisRelayRegion;
        // empty relay router.
        this.router = new RelayRouter(metricRegistry, type);
        // load initial configuration if available.
        // if error happens during reload() we still have empty router that can be updated at a later time.
        reload();
        log.info( "Relay started" );
    }

    @PreDestroy
    public void stop()
    {
        log.info( String.format("[%s] Stopping relay", type) );
        close( router.getDestinations() );
        log.info( String.format("[%s] Relay stopped", type) );
    }

    @Override
    public void accept( DataPoints points )
    {
        if( router.isEmpty() )
        {
            return;
        }

        DataPoint t;
        for(int i = 0, n = points.size(); i < n; i++)
        {
            t = points.get( i );
            if( t.isValid() )
            {
                router.accept( t );
            }
        }
    }

    public void accept( DataPoint dataPoint )
    {
        router.accept( dataPoint );
    }

    static void close( Collection<DestinationGroup> groups )
    {
        if ( null == groups )
        {
            return;
        }
        groups.forEach( DestinationGroup::close);
    }

    // synchronized to make sure that only one reload at a time.
    public synchronized void reload()
    {
        if ( log.isDebugEnabled() )
        {
            log.debug( String.format("[%s] Check for configuration update", type) );
        }
        try
        {
            RelayRules newRules = new RelayRules(type, rulesFile, rulesSrc, relayCacheEnabled, configServerUtil);
            RelayRules currentRules = router.getRules();

            if ( log.isDebugEnabled() )
            {
                log.debug( String.format("[%s] Check for configuration update", type) );
            }

            if( !Objects.equals(newRules, currentRules ))
            {
                log.info( String.format("[%s] Updating Relay Rules. Old relay rules: [%s], New relay rules: [%s]", type, currentRules, newRules) );
            }
            else
            {
                log.debug( String.format("%s Relay rules haven't changed.", type) );
                return;
            }

            reconfigureRelayRouter( newRules );
            StringsCache.invalidateCache();
        }
        catch ( Exception e )
        {
            log.error( String.format("[%s] Failed to reload config. Suppress. ", type), e );
        }
    }

    private void reconfigureRelayRouter( RelayRules newRules )
    {
        Set<String> newIDs = newRules.allDestinationGroups();
        Set<DestinationGroup> destinationGroups = router.getDestinations();
        Set<DestinationGroup> newDGs = new HashSet<>(  );
        Set<DestinationGroup> obsoleteDGs = new HashSet<> ();

        for(DestinationGroup dg : destinationGroups)
        {
            String destAsTxt = dg.getDest();
            if( newIDs.contains( destAsTxt ))
            {
                log.info( String.format("[%s] Reuse unchanged destination group: %s", type, destAsTxt) );
                newDGs.add( dg );
                newIDs.remove( destAsTxt );
            }
            else
            {
                log.info( String.format("[%s] Destination group scheduled for removal: %s", type, destAsTxt) );
                obsoleteDGs.add( dg );
            }
        }

        // create new destination groups
        for(String destAsTxt : newIDs)
        {
            log.info( String.format("[%s] Creating new destination group: %s", type, destAsTxt) );
            DestinationGroup dg = new DestinationGroup(metricRegistry, type, destAsTxt, queueSize, batchSize, refreshIntervalInMillis, destConfigDir, maxWaitTimeInMillis, kinesisRelayRegion);
            newDGs.add( dg );
        }

        RelayRouter newRouter = new RelayRouter( metricRegistry, type, newRules, newDGs );

        if( newDGs.size() == 0 )
        {
            log.warn( String.format("[%s] No relay destinations configured.", type) );
        }

        // replace router
        this.router = newRouter;

        // close obsolete destinations
        close( obsoleteDGs );
    }

    void dumpStats()
    {
        router.dumpStats();
    }

}
