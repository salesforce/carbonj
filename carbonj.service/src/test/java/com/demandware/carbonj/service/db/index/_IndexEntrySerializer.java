/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.db.model.RetentionPolicy;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class _IndexEntrySerializer
{
    private NameRecordSerializer serializer = new NameRecordSerializer();

    @Before
    public void setUp()
    {
    }

    @Test
    public void shouldSerializeNonLeafNodeWithoutChildNodes()
    {
        NameRecord e = new NameRecord( "root", 1, false );
        byte[] keyBytes = serializer.keyBytes( e.key() );
        byte[] valueBytes = serializer.valueBytes( e );

        assertIndexEntriesEqual( e, serializer.toIndexEntry( keyBytes, valueBytes ) );
    }

    @Test
    public void shouldSerializeNonLeafNodeWithOneChildNode()
    {
        NameRecord e = new NameRecord( "root", 1, false );
        e.addChildKeyIfMissing( "child" );

        byte[] keyBytes = serializer.keyBytes( e.key() );
        byte[] valueBytes = serializer.valueBytes( e );

        assertIndexEntriesEqual( e, serializer.toIndexEntry( keyBytes, valueBytes ) );
    }

    @Test
    public void shouldSerializeNonLeafNodeWithMultipleChildNode()
    {
        NameRecord e = new NameRecord( "root", 1, false );
        e.addChildKeyIfMissing( "child1" );
        e.addChildKeyIfMissing( "child2" );
        e.addChildKeyIfMissing( "child3" );
        e.addChildKeyIfMissing( "child4" );

        byte[] keyBytes = serializer.keyBytes( e.key() );
        byte[] valueBytes = serializer.valueBytes( e );

        assertIndexEntriesEqual( e, serializer.toIndexEntry( keyBytes, valueBytes ) );
    }


    @Test
    public void shouldSerializeLeafNodeWithoutChildNodes()
    {
        NameRecord e = new NameRecord( "carbon.metric.a", 1, true);
        e.setRetentionPolicies( RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y") );
        byte[] keyBytes = serializer.keyBytes( e.key() );
        byte[] valueBytes = serializer.valueBytes( e );

        //TODO: create custom matcher
        assertIndexEntriesEqual( e, serializer.toIndexEntry( keyBytes, valueBytes ) );
    }

    private void assertIndexEntriesEqual( NameRecord e, NameRecord a)
    {
        assertThat(e.getKey(), equalTo(a.getKey()));
        assertThat(e.getId(), equalTo(a.getId()));
        assertThat(e.isLeaf(), equalTo( a.isLeaf() ));
        assertThat(e.getChildren(), equalTo(a.getChildren()));
        assertThat(e.getRetentionPolicies(), equalTo(a.getRetentionPolicies()));
    }

}
