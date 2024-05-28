/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

public class RetentionPolicy
{
    private static final ConcurrentMap<String, List<RetentionPolicy>> policyLists = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, RetentionPolicy> policies = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, String> dbNameToPolicyNameMap = new ConcurrentHashMap<>();

    private static final String _60s24h = "60s24h";
    private static final String _60s30d = "60s30d";
    private static final String _5m7d = "5m7d";
    private static final String _30m2y = "30m2y";

    /**
     * Policy name
     */
    public final String name;

    /**
     * Number of seconds represented by one data point.
     */
    public final int precision;

    /**
     * How long data should be kept.
     */
    public final int retention;

    public final String dbName;

    private RetentionPolicy( String name )
    {
        this.name = Preconditions.checkNotNull( name );
        String[] parts = name.split( ":" );
        precision = toSeconds( parts[0] );
        retention = toSeconds( parts[1] );
        dbName = name.replace( ":", "" );
    }

    public void assertTimestampMatchesThisPolicyInterval( int ts )
    {
        int ts_interval = interval( ts );
        if ( ts_interval != ts )
        {
            throw new RuntimeException( String.format(
                "timestamp does not match any interval from this retention policy. "
                    + "ts: %s, retention policy interval for ts: %s", ts, ts_interval ) );
        }
    }

    public boolean includes( int ts, int now )
    {
        return interval( now - retention ) <= interval( ts );
    }

    public int interval( int ts )
    {
        return ts - ( ts % precision );
    }

    public static List<RetentionPolicy> getPolicyList( String retentionLine )
    {
        List<RetentionPolicy> list = policyLists.get( retentionLine );
        if ( list == null )
        {
            list = policyLists.computeIfAbsent( retentionLine, RetentionPolicy::newPolicyList);
        }
        return list;
    }

    private static List<RetentionPolicy> newPolicyList( String retentionLine )
    {
        return Arrays.stream( retentionLine.split( "," ) ).map(RetentionPolicy::getInstance)
            .collect( Collectors.toList() );
    }

    public static boolean dbNameExists( String dbName )
    {
        return dbNameToPolicyNameMap.containsKey( dbName );
    }

    public static RetentionPolicy _60s24h()
    {
        return RetentionPolicy.getInstanceForDbName( _60s24h );
    }

    public boolean is60s24h()
    {
        return _60s24h.equals( dbName );
    }

    public boolean is60s30d()
    {
        return _60s30d.equals( dbName );
    }

    public boolean is5m7d()
    {
        return _5m7d.equals( dbName );
    }

    public boolean is30m2y()
    {
        return _30m2y.equals( dbName );
    }

    public static RetentionPolicy getInstance( String name )
    {
        RetentionPolicy p = policies.get( name );
        if ( p == null )
        {
            p = policies.computeIfAbsent( name, RetentionPolicy::new);
            dbNameToPolicyNameMap.putIfAbsent(p.dbName, p.name);
        }
        return p;
    }

    public static RetentionPolicy getInstanceForDbName( String dbName )
    {
        String rpName = dbNameToPolicyNameMap.get( dbName );
        return getInstance( rpName );
    }

    private static int toSeconds( String value )
    {
        char suffix = value.charAt( value.length() - 1 );
        int t = Integer.parseInt( value.substring( 0, value.length() - 1 ) );
        return switch (suffix) {
            case 's' -> t;
            case 'm' -> t * 60;
            case 'h' -> t * 3600;
            case 'd' -> t * 3600 * 24;
            case 'y' -> t * 3600 * 24 * 365;
            default ->
                    throw new IllegalArgumentException("Unsupported time unit suffix [" + suffix + "] in value [" + value
                            + "]");
        };
    }

    public static Optional<RetentionPolicy> pickArchiveForQuery(int from, int until, int now)
    {
        return policies.values().stream().filter( p -> p.includes( from, now ) && p.includes( until, now ) )
                .reduce(RetentionPolicy::higherPrecision);
    }

    public static RetentionPolicy higherPrecision(RetentionPolicy a, RetentionPolicy b)
    {
        if( a == null )
        {
            return b;
        }

        if( b == null )
        {
            return a;
        }

        if( a.precision <= b.precision )
        {
            return a;
        }
        else
        {
            return b;
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof RetentionPolicy that) )
        {
            return false;
        }

        return name.equals( that.name );

    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public String toString()
    {
        return "RetentionPolicy{" + "name='" + name + '\'' + ", precision=" + precision + ", retention=" + retention
            + '}';
    }

    public int maxPoints(int from, int until, int now) {
        if (until > now) {
            until = now;
        }
        int startTs = Math.max(now - retention, from);

        if (startTs > until) {
            return 0;
        }

        return  (until - startTs) / precision;
    }
}
