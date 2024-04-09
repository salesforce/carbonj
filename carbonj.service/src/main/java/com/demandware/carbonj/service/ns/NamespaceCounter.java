/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.ns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceCounter
{
    private static final Logger log = LoggerFactory.getLogger( NamespaceCounter.class );

    private final ConcurrentHashMap<String, Meter> namespaceMeters = new ConcurrentHashMap<>(  );

    private final MetricRegistry metricRegistry;

    private final Meter removedMeter;

    private final Meter addedMeter;

    // Entry is <namespace, timestampToRemoveAt>
    private final Map<String, Integer> candidatesForRemoval = new HashMap<>();

    /*
      Grace period for not receiving any data before the name is removed.

      Note: metrics that are sent once in a couple of hours won't be affected
      because this class track top level names (e.g. pod21, pi, ...) for expiration
      and typically there will be other metrics that are sent frequently enough.
     */
    private final int removeInactiveAfterSec;

    public NamespaceCounter(MetricRegistry metricRegistry, int removeInactiveAfterSec)
    {
        this.metricRegistry = metricRegistry;
        this.removeInactiveAfterSec = removeInactiveAfterSec;
        this.removedMeter  = metricRegistry.meter(
                MetricRegistry.name( "namespaces.all.expired" ) );

        this.addedMeter  = metricRegistry.meter(
                MetricRegistry.name( "namespaces.all.added" ) );

    }

    public void count(String name)
    {
        String ns = namespace(name);

        Meter m = namespaceMeters.get( ns );
        if( m == null )
        {
            m = namespaceMeters.computeIfAbsent( ns, key ->  addNamespace(key, name));
        }
        m.mark();
    }

    private Meter addNamespace(String ns, String name)
    {
        Meter meter = metricRegistry.meter( MetricRegistry.name( "namespaces.ns", ns ) );
        log.info(String.format("added new namespace: [%s] based on metric [%s]", ns, name));
        addedMeter.mark();
        return meter;
    }

    public boolean exists(String name)
    {
        return namespaceMeters.containsKey( namespace(name) );
    }

    public String namespace(String name)
    {
        int i = name.indexOf( '.' );
        return i > 0 ? name.substring( 0, i) : name;
    }

    public Set<String> getLiveNamespaces() {
        Set<String> namespaces = namespaceMeters.keySet();
        for (String toRemove : candidatesForRemoval.keySet()) {
            namespaces.remove(toRemove);
        }
        return namespaces;
    }

    public void removeInactive()
    {
        try
        {
            log.info("running clean up of inactive namespace counters");
            int now = Math.toIntExact(System.currentTimeMillis() / 1000);
            pickCandidatesForRemoval(now);
            removeEntriesWithExpiredValues(now);
        }
        catch(Throwable t)
        {
            log.error("Unexpected exception when trying to remove obsolete namespace counters.", t);
        }
    }

    private void pickCandidatesForRemoval(int now)
    {
        ConcurrentHashMap<String, Meter> copy = new ConcurrentHashMap<>(namespaceMeters);
        log.info(String.format("Current namespaces: %s", copy.keySet()));
        log.info(String.format("Current namespaces tracked for removal: %s", candidatesForRemoval.keySet()));
        copy.forEach( ( k, v ) -> {
            if (isInactive(k, v))
            {
                if( candidatesForRemoval.putIfAbsent(k, now + removeInactiveAfterSec) == null)
                {
                    log.info(String.format("started to track namespace [%s] for removal", k));
                }
            }
            else
            {
                if( candidatesForRemoval.remove(k) != null )
                {
                    log.info(String.format("removed namespace [%s] from removal candidates", k));
                }
            }
        });

    }

    private void removeEntriesWithExpiredValues(int now)
    {
        List<String> toRemove = new ArrayList<>();
        candidatesForRemoval.forEach( (k, v) -> {
            if( v <= now )
            {
                toRemove.add(k);
            }
        });

        toRemove.forEach( key -> {
            namespaceMeters.remove(key);
            metricRegistry.remove(key);
            candidatesForRemoval.remove(key);
            log.info(String.format("removed inactive namespace counter with key: [%s]", key));
        } );
        int removedCount = toRemove.size();
        removedMeter.mark(removedCount);
        log.info(String.format("finished clean up of expired namespace counters. total removed: %s", removedCount));
    }

    private boolean isInactive(String name, Meter m)
    {
        // there shouldn't be too m
        double inactiveThreshold = 0.1;
        log.info(String.format("namespace: [%s], 15minRate: [%s], threshold: [%s]", name, m.getFifteenMinuteRate(), inactiveThreshold));
        return m.getFifteenMinuteRate() < inactiveThreshold;
    }

}
