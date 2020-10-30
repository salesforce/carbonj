/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

import com.demandware.carbonj.service.db.model.RetentionPolicy;

class NameRecord
                implements Record<String>
{
    private String key;

    //not used for non-leaf nodes
    //for leaf nodes provides unique metric id
    //in root node contains next available metric id.
    //to start using brute-force approach with counter stored in root record.
    private long id;

    private List<String> children;

    private List<RetentionPolicy> retentionPolicies;

    private boolean isLeaf;

    private static String ROOT_KEY = InternalConfig.getRootEntryKey();

    @Override
    public String key()
    {
        return key;
    }

    public NameRecord( String key, long id, boolean isLeaf)
    {
        this.key = key;
        this.id = id;
        // use null instead of empty array list - leaves will be cached and empty array list will waste memory.
        this.children = null;
        this.isLeaf = isLeaf;

    }

    public void setChildren( List<String> children )
    {
        this.children = children;
    }

    public boolean isLeaf()
    {
        return isLeaf;
    }

    public List<RetentionPolicy> getRetentionPolicies()
    {
        return retentionPolicies;
    }

    public void setRetentionPolicies( List<RetentionPolicy> retentionPolicies )
    {
        this.retentionPolicies = retentionPolicies;
    }

    public List<String> getChildren()
    {
        return children == null ? Collections.EMPTY_LIST : children;
    }

    public boolean removeChildKeyIfExists(String childKey)
    {
        boolean isRootKey = ROOT_KEY.equals( key );

        if( !isRootKey )
        {
            Preconditions.checkState( childKey.startsWith( key ),
                "Child key should share prefix with parent. parent [%s], child: [%s]", key, childKey );
        }
        String suffix = isRootKey ? childKey : childKeySuffix( childKey );
        Preconditions.checkState(suffix.indexOf( '.' ) < 0, "Invalid child key - has more than one level. parent [%s], child: [%s]", key, childKey);

        if( children == null )
        {
            return false;
        }

        return children.remove( suffix );
    }

    public boolean addChildKeyIfMissing(String childKey)
    {
        boolean isRootKey = ROOT_KEY.equals( key );

        if( !isRootKey )
        {
            Preconditions.checkState( childKey.startsWith( key ),
                            "Child key should share prefix with parent. parent [%s], child: [%s]", key, childKey );
        }
        String suffix = isRootKey ? childKey : childKeySuffix( childKey );
        Preconditions.checkState(suffix.indexOf( '.' ) < 0, "Invalid child key - has more than one level. parent [%s], child: [%s]", key, childKey);

        if( children == null )
        {
            children = new ArrayList<>(  );
        }

        if( children.contains( suffix ) )
        {
            // already exists
            return false;
        }
        else
        {
            children.add( suffix );
            return true;
        }
    }

    private String childKeySuffix(String childKey)
    {
        return key.length() == 0 ? childKey : childKey.substring( key.length() + 1 );
    }

    public String getKey()
    {
        return key;
    }

    public long getId()
    {
        return id;
    }

    public void setId( long id )
    {
        this.id = id;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !( o instanceof NameRecord ) )
            return false;

        NameRecord that = (NameRecord) o;

        return key.equals( that.key );

    }

    @Override
    public int hashCode()
    {
        return key.hashCode();
    }

    @Override
    public String toString()
    {
        return this.key + ":" +
                        "id=" + this.id +
                        ",isLeaf=" + this.isLeaf +
                        ",children=" + this.children +
                        ",retentionPolicies=" + this.retentionPolicies;
    }

}
