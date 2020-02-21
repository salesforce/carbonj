/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.demandware.carbonj.service.db.model.Metric;

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
        assertThat(deleted.size(), equalTo(1));

        // verify it doesn't exist anymore
        assertMetricsDoNotExist( asList("a"), asList(aMetric.id) );

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
        assertThat(deleted.size(), equalTo(3));

        assertMetricsDoNotExist(asList("a", "a.b", "a.b.c"), asList(aMetric.id) );

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
        assertThat(deleted.size(), equalTo(3));

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
        assertThat(deleted.size(), equalTo(1));

        assertMetricsDoNotExist( asList("a.b.c"), asList(cMetric.id, cMetric.id));

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
        assertThat(deleted.size(), equalTo(3));

        assertMetricsExist( asList("a.b", "a.b.c", "a.b.d"), asList(cMetric.id, dMetric.id));

        // verify that previous metric will be reused
        assertThat(findOrCreate( "a.b.c").id, equalTo(cMetric.id));
        assertThat(findOrCreate( "a.b.d").id, equalTo(dMetric.id));
    }


    private void assertMetricsDoNotExist(List<String> names, List<Integer> ids)
    {
        ids.forEach( id -> assertThat( index.getMetric( id ), nullValue() ) );

        names.forEach( name -> {
            if( nameUtils.isTopLevel(name) )
            {
                assertThat(index.getTopLevelNames(), not(hasItem(name)));
            }

            assertThat(index.getMetric( name ), nullValue() );
            assertThat(index.findMetrics( name ), emptyCollectionOf(Metric.class) );
        } );
    }

    private void assertMetricsExist(List<String> names, List<Integer> ids)
    {
        ids.forEach( id -> assertThat( index.getMetric( id ), not(nullValue()) ) );

        names.forEach( name -> {
            if( nameUtils.isTopLevel(name) )
            {
                assertThat(index.getTopLevelNames(), hasItem(name));
            }

            assertThat(index.getMetric( name ), not(nullValue()) );
            assertThat(index.findMetrics( name ), not(emptyCollectionOf(Metric.class)) );
        } );
    }

    private void assertCanCreateAgain(String name, int originalId)
    {
        Metric bMetric = findOrCreate( name );
        assertTrue(originalId < bMetric.id);

        assertThat( index.getMetric( bMetric.id ), equalTo(bMetric) );
        assertThat( index.getMetric( bMetric.name ), equalTo(bMetric) );
        assertThat( index.findMetrics( bMetric.name ), equalTo( singletonList(bMetric) ) );
    }
}
