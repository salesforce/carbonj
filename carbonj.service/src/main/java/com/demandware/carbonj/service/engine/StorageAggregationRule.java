/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * Single rule.
 */
class StorageAggregationRule
{
    final private Pattern pattern;
    final AggregationMethod method;
    final private boolean alwaysTrue;
    private static final Logger log = LoggerFactory.getLogger( StorageAggregationRule.class );

    public static StorageAggregationRule parseDefinition( String line)
    {
        String[] parts = line.split( "=", 2 );
        String m = parts[0].trim();
        String p = parts[1].trim();

        return new StorageAggregationRule(p, AggregationMethod.valueOf( m.toUpperCase() ) );
    }


    public StorageAggregationRule( String p, AggregationMethod m)
    {
        this.method = Preconditions.checkNotNull( m );
        if( "*".equals( p.trim() ) )
        {
            this.alwaysTrue = true;
            this.pattern = null;
        }
        else
        {
            this.alwaysTrue = false;
            this.pattern = Pattern.compile( p );
        }
    }

    public AggregationMethod apply(String name)
    {
        if( alwaysTrue )
        {
            return method;
        }
        log.info("Get the metric name for single metric: " + name);
        Matcher m = pattern.matcher( name );
        return m.find() ? method : null;
    }

    public AggregationMethod getMethod()
    {
        return method;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper( this )
                      .add( "pattern", pattern )
                      .add( "method", method )
                      .toString();
    }
}
