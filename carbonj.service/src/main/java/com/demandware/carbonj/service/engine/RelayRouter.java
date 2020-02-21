/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Links relay rules with actual destinations. Main purpose is to support atomic switching of configuration within Relay.
 *
 * Immutable. Relay will create a new RelayRouter instance when configuration changes.
 *
 * To minimize disruption during reconfiguration destinations that haven't changed will be switched to a new RelayRouter
 * instance.
 */
public class RelayRouter implements Consumer<DataPoint>
{
    private static Logger log = LoggerFactory.getLogger( RelayRouter.class );

    private String type;

    Meter received;
    Meter drop;
    Meter sent;
    Meter routerDrop;

    private final RelayRules rules;
    private final Map<String, DestinationGroup> destinationsMap;
    private final boolean empty;

    public RelayRouter(MetricRegistry metricRegistry, String type)
    {
        this(metricRegistry, type, new RelayRules(type), new HashSet<>(  ));
    }

    public RelayRouter(MetricRegistry metricRegistry, String type, RelayRules rules, Set<DestinationGroup> destinations)
    {
        this.type = type;
        this.rules = rules;
        this.empty = rules.isEmpty();
        this.destinationsMap = new HashMap<>();
        for(DestinationGroup dg : destinations)
        {
            destinationsMap.put( dg.getDest(), dg );
        }

        this.received = metricRegistry.meter( MetricRegistry.name( type, "recv" ) );
        drop = metricRegistry.meter( MetricRegistry.name( type, "drop" ) );
        sent = metricRegistry.meter( MetricRegistry.name( type, "sent" ) );
        routerDrop = metricRegistry.meter( MetricRegistry.name( type, "drop" ) );
    }

    public boolean isEmpty()
    {
        return empty;
    }

    Set<DestinationGroup> getDestinations()
    {
        return new HashSet<>(destinationsMap.values());
    }

    public RelayRules getRules()
    {
        return rules;
    }

    @Override
    public void accept( DataPoint t )
    {
        if( isEmpty() )
        {
            return;
        }

        received.mark();


        String[] destGroups = rules.getDestinationGroups( t.name );
        if( destGroups.length == 0 )
        {
            routerDrop.mark();
            if ( log.isDebugEnabled() )
            {
                log.debug( "no destination matched -> drop. Total dropped " + drop.getCount() );
            }
            return;
        }
        else
        {
            for ( String id : destGroups )
            {
                DestinationGroup dg = destinationsMap.get( id );
                dg.accept( t );
            }
        }
    }

    public void dumpStats()
    {
        log.info( String.format("router[%s]: points=%s, rate=%s, dropped=%s", this.type,
                        this.received.getCount(), this.received.getMeanRate(), routerDrop.getCount()) );
    }
}
