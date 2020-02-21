/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.python.google.common.base.Preconditions;

import com.demandware.carbonj.service.db.util.Quota;

public class NameUtils
{
    private static final Logger log = LoggerFactory.getLogger( NameUtils.class );

    final private String rootKey;

    final private String rootKeyPrefix;

    final private Quota nameErrLogQuota;

    NameUtils( String rootKey, Quota nameErrLogQuota )
    {
        this.rootKey = rootKey;
        this.rootKeyPrefix = rootKey + ".";
        this.nameErrLogQuota = Preconditions.checkNotNull( nameErrLogQuota );
    }

    public NameUtils()
    {
        this( InternalConfig.getRootEntryKey() );
    }

    public NameUtils( Quota nameErrLogQuota )
    {
        this( InternalConfig.getRootEntryKey(), nameErrLogQuota );
    }

    NameUtils( String rootKey )
    {
        this( rootKey, new Quota( 0, 0 )
        {
            // TBD - just a simple hack for now - by default not log errors
            @Override
            public boolean allow()
            {
                return false;
            }
        } );
    }

    public boolean isValid( String name )
    {
        return isValid( name, nameErrLogQuota.allow() );
    }

    public boolean isValid( String name, boolean verbose )
    {
        if ( name == null )
        {
            if ( verbose )
            {
                log.warn( "Invalid metric name - [null]." );
            }
            return false;
        }

        if ( name.length() == 0 )
        {
            if ( verbose )
            {
                log.warn( "Invalid metric name - [\"\"]." );
            }
            return false;
        }

        if ( name.startsWith( rootKeyPrefix ) )
        {
            if ( verbose )
            {
                log.warn( String.format( "Metric name [%s] is invalid because it starts with reserved prefix [%s]",
                    name, rootKeyPrefix ) );
            }
            return false;
        }

        boolean valid = true;
        char prev;
        char ch = '.';
        for ( int i = 0, n = name.length(); i < n; i++ )
        {
            prev = ch;
            ch = name.charAt( i );

            // a-z
            if ( ch >= 0x61 && ch <= 0x7A )
            {
                continue;
            }

            // A-Z
            if ( ch >= 0x41 && ch <= 0x5A )
            {
                continue;
            }

            // 0-9
            if ( ch >= 0x30 && ch <= 0x39 )
            {
                continue;
            }

            // doesn't start with '.', and doesn't have '..' in the middle
            if ( ch == '.' && prev != '.' )
            {
                continue;
            }

            if ( ch == '_' || ch == '-' || ch == ':' || ch == '=' || ch == '%' )
            {
                continue;
            }

            // invalid character
            if ( verbose )
            {
                log.warn( String.format( "Metric name [%s] contains invalid character [%s] in pos [%s]", name, ch, i ) );
            }
            valid = false;
            break;
        }

        // name ends with '.'
        if ( valid && ch == '.' )
        {
            if ( verbose )
            {
                log.warn( String.format( "Metric name [%s] is invalid because it ends with '.'", name ) );
            }
            valid = false;
        }

        return valid;
    }

    public boolean isTopLevel( String name )
    {
        return name.indexOf( '.' ) == -1;
    }

    public String firstSegment( String name )
    {
        int i = name.indexOf( '.' );
        return i < 0 ? name : name.substring( 0, i );
    }

    public Optional<String> parentName( String name )
    {
        int i = name.lastIndexOf( '.' );
        return i < 0 ? Optional.empty() : Optional.of( name.substring( 0, i ) );
    }

    public String[] metricNameHierarchy( String name )
    {
        // brute force
        String[] parts = name.split( "\\." );
        String[] paths = new String[parts.length];
        for ( int i = 0; i < paths.length; i++ )
        {
            if ( i > 0 )
            {
                paths[i] = paths[i - 1] + "." + parts[i];
            }
            else
            {
                paths[i] = parts[i];
            }

        }
        return paths;
    }

    /**
     * Assembles metric name from (parent, child) segments and skips parent if it is a root key.
     */
    public String toMetricName( String parentPath, String childPath )
    {
        return rootKey.equals( parentPath ) ? childPath : parentPath + "." + childPath;
    }
}
