/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.demandware.carbonj.service.db.model.Metric;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for metric name delete operation from name index.
 */
public class _DeleteNameFromIndex extends BaseIndexTest
{
    /**
     * Tests removal of metric name that has just one segment (i.e. it is a leaf and top-level name at the same time)
     */
    @Test
    public void deleteLeafFromTopLevel() throws Exception
    {
        index.setStrictMode( true );

        // create metric
        Metric aMetric = findOrCreate( "a" );

        // delete
        List<Metric> deleted = index.deleteMetric( "a", false, false );
        assertEquals(deleted.size(), 1);

        // verify it doesn't exist anymore
        assertMetricsDoNotExist(List.of("a"), List.of(aMetric.id));

        // verify that can create it again and different id will be assigned
        assertCanCreateAgain("a", aMetric.id);
    }

    /**
     * This test removes top level metric name (one of the roots) and verifies that this name and all of its descendants
     * are no longer present in the index.
     */
    @Test
    public void deleteSubtreeFromTopLevel() throws Exception
    {
        // causes exception if any data inconsistency is detected instead of skipping invalid data.
        index.setStrictMode( true );

        Metric aMetric = findOrCreate( "a.b.c" );
        List<Metric> deleted = index.deleteMetric( "a", true, false );
        assertEquals(deleted.size(), 3);

        assertMetricsDoNotExist(asList("a", "a.b", "a.b.c"), List.of(aMetric.id));

        assertCanCreateAgain("a.b.c", aMetric.id);
    }

    @Test
    public void deleteSubtreeBelowTopLevel() throws Exception
    {
        // causes exception if any data inconsistency is detected instead of skipping invalid data.
        index.setStrictMode( true );

        Metric cMetric = findOrCreate( "a.b.c" );
        Metric dMetric = findOrCreate( "a.b.d" );
        List<Metric> deleted = index.deleteMetric( "a.b", true, false );
        assertEquals(deleted.size(), 3);

        assertMetricsDoNotExist( asList("a.b", "a.b.c", "a.b.d"), asList(cMetric.id, dMetric.id));

        assertCanCreateAgain("a.b.c", cMetric.id);
        assertCanCreateAgain("a.b.d", dMetric.id);
    }

    @Test
    public void deleteLeafBelowTopLevel() throws Exception
    {
        // causes exception if any data inconsistency is detected instead of skipping invalid data.
        index.setStrictMode( true );

        Metric cMetric = findOrCreate( "a.b.c" );
        List<Metric> deleted = index.deleteMetric( "a.b.c", true, false );
        assertEquals(deleted.size(), 1);

        assertMetricsDoNotExist(List.of("a.b.c"), asList(cMetric.id, cMetric.id));

        assertCanCreateAgain("a.b.c", cMetric.id);
    }

    @Test
    public void deleteSubtreeBelowTopLevelInTestMode() throws Exception
    {
        // causes exception if any data inconsistency is detected instead of skipping invalid data.
        index.setStrictMode( true );

        Metric cMetric = findOrCreate( "a.b.c" );
        Metric dMetric = findOrCreate( "a.b.d" );
        List<Metric> deleted = index.deleteMetric( "a.b", true, true );
        assertEquals(deleted.size(), 3);

        assertMetricsExist( asList("a.b", "a.b.c", "a.b.d"), asList(cMetric.id, dMetric.id));

        // verify that previous metric will be reused
        assertEquals(findOrCreate( "a.b.c").id, cMetric.id);
        assertEquals(findOrCreate( "a.b.d").id, dMetric.id);
    }


    private void assertMetricsDoNotExist(List<String> names, List<Long> ids)
    {
        ids.forEach( id -> assertNull( index.getMetric( id ) ) );

        names.forEach( name -> {
            if( nameUtils.isTopLevel(name) )
            {
                assertFalse(index.getTopLevelNames().contains(name));
            }

            assertNull(index.getMetric( name ));
            assertTrue(index.findMetrics( name ).isEmpty());
        } );
    }

    private void assertMetricsExist(List<String> names, List<Long> ids)
    {
        ids.forEach( id -> assertNotNull( index.getMetric( id ) ));

        names.forEach( name -> {
            if( nameUtils.isTopLevel(name) )
            {
                assertTrue(index.getTopLevelNames().contains(name));
            }

            assertNotNull(index.getMetric( name ));
            assertFalse(index.findMetrics( name ).isEmpty() );
        } );
    }

    private void assertCanCreateAgain(String name, long originalId)
    {
        Metric bMetric = findOrCreate( name );
        assertTrue(originalId < bMetric.id);

        assertEquals( index.getMetric( bMetric.id ), bMetric );
        assertEquals( index.getMetric( bMetric.name ), bMetric );
        assertEquals( index.findMetrics( bMetric.name ), singletonList(bMetric) );
    }
}
