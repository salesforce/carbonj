/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.demandware.carbonj.service.db.model.RetentionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class _IndexEntrySerializer
{
    private final NameRecordSerializer serializer = new NameRecordSerializer(false);

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
        assertEquals(e.getKey(), a.getKey());
        assertEquals(e.getId(), a.getId());
        assertEquals(e.isLeaf(), a.isLeaf());
        assertEquals(e.getChildren(), a.getChildren());
        assertEquals(e.getRetentionPolicies(), a.getRetentionPolicies());
    }

    @Test
    public void deserializerNameRecord() {
        String key = "pod222.ecom_ag.bjmr.bjmr_prd.search.inventory-support.filter-calculation.filter-update-job.number-of-filters";
        String hexValue = "01000000029544FEA00000000000000000000300073630733A3234680005356D3A3764000633306D3A3279";
        byte[] valueBytes = new byte[hexValue.length() / 2];
        for (int i = 0; i < valueBytes.length; i++) {
            int index = i * 2;
            int value = Integer.parseInt(hexValue.substring(index, index + 2), 16);
            valueBytes[i] = (byte) value;
        }
        NameRecord nameRecord = new NameRecordSerializer(true).toIndexEntry(key, valueBytes);
        assertEquals(key, nameRecord.getKey());
        assertEquals(11094261408L, nameRecord.getId());
        assertTrue(nameRecord.getChildren().isEmpty());
        assertTrue(nameRecord.isLeaf());
        assertEquals(3, nameRecord.getRetentionPolicies().size());
    }
}
