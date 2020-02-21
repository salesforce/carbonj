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

    private static final String _60s24h = "60s24h";
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

    public boolean includes2( int ts, int now ) // TODO: keeping it for now to minimize impact on existing logic
    {
        return now - retention <= interval( ts );
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
            list = policyLists.computeIfAbsent( retentionLine, key -> newPolicyList( key ) );
        }
        return list;
    }

    private static List<RetentionPolicy> newPolicyList( String retentionLine )
    {
        return Arrays.stream( retentionLine.split( "," ) ).map( v -> RetentionPolicy.getInstance( v ) )
            .collect( Collectors.toList() );
    }

    public static boolean policyNameExists( String name )
    {
        return policies.containsKey( name );
    }

    public static boolean dbNameExists( String dbName )
    {
        String policyName = dbNameToPolicyName( dbName );
        return policies.containsKey( policyName );
    }

    public static RetentionPolicy _60s24h()
    {
        return RetentionPolicy.getInstanceForDbName( _60s24h );
    }

    public boolean is60s24h()
    {
        return _60s24h.equals( dbName );
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
            p = policies.computeIfAbsent( name, ( key ) -> new RetentionPolicy( key ) );
        }
        return p;
    }

    public static RetentionPolicy getInstanceForDbName( String dbName )
    {
        String rpName = dbNameToPolicyName( dbName );
        return getInstance( rpName );
    }

    private static String dbNameToPolicyName( String dbName )
    {
        int precisionEnd = 0;
        for ( int i = 0; i < dbName.length(); i++ )
        {
            int ch = dbName.charAt( i );
            if ( ch == 's' || ch == 'm' || ch == 'h' || ch == 'd' || ch == 'y' )
            {
                precisionEnd = i + 1;
                break;
            }
        }
        if ( precisionEnd == 0 || precisionEnd >= dbName.length() )
        {
            throw new RuntimeException( String.format( "Failed to convert dbname [%s] to policy name", dbName ) );
        }

        return dbName.substring( 0, precisionEnd ) + ":" + dbName.substring( precisionEnd );
    }

    private static int toSeconds( String value )
    {
        char suffix = value.charAt( value.length() - 1 );
        int t = Integer.parseInt( value.substring( 0, value.length() - 1 ) );
        switch ( suffix )
        {
            case 's':
                return t;
            case 'm':
                return t * 60;
            case 'h':
                return t * 3600;
            case 'd':
                return t * 3600 * 24;
            case 'y':
                return t * 3600 * 24 * 365;
            default:
                throw new IllegalArgumentException( "Unsupported time unit suffix [" + suffix + "] in value [" + value
                    + "]" );
        }
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
        if ( !( o instanceof RetentionPolicy ) )
        {
            return false;
        }

        RetentionPolicy that = (RetentionPolicy) o;

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
