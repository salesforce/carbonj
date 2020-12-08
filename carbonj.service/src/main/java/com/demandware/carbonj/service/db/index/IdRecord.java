/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.google.common.base.Preconditions;

class IdRecord
                implements Record<Long>
{
    private Long key;

    private String metricName;

    public IdRecord( Long key, String metricName)
    {
        this.key = Preconditions.checkNotNull(key);
        this.metricName = Preconditions.checkNotNull(metricName);
    }

    public Long key()
    {
        return key;
    }

    public String metricName()
    {
        return metricName;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !( o instanceof IdRecord ) )
            return false;

        IdRecord that = (IdRecord) o;

        if ( !key.equals( that.key ) )
            return false;
        return metricName.equals( that.metricName );

    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + metricName.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return this.key + ":" + this.metricName;
    }
}
