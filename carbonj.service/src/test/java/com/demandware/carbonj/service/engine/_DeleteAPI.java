/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.model.DeleteAPIResult;
import com.demandware.carbonj.service.db.model.Metric;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class _DeleteAPI extends AbstractCarbonJ_StoreTest
{
    @Test
    public void deleteLeafTest() throws IOException {
        cjClient.send( "a.b.c", 1.0f, new DateTime() );
        cjClient.send( "a.b.d", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.f", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.g", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete1 = carbonjAdmin.deleteAPI("a.b.c", true, Collections.EMPTY_LIST);
        DeleteAPIResult resultDelete2 = carbonjAdmin.deleteAPI("a.b.e", true, Collections.EMPTY_LIST);

        assertEquals( 1L, resultDelete1.getLeafCount() );
        assertEquals( 1L, resultDelete1.getTotalCount() );
        assertEquals( 2L, resultDelete2.getLeafCount() );
        assertEquals( 3L, resultDelete2.getTotalCount() );
        assertEquals( Arrays.asList(  "a", "a.b", "a.b.d" ), carbonjAdmin.findAllMetrics( "a") );
    }


    @Test
    public void deleteWithExcludeTest() throws IOException {
        cjClient.send( "a.b.e.f", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.g", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.h", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.i", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("a.b.e", true, Arrays.asList("f", "i"));
        assertEquals( 2L, resultDelete.getLeafCount() );
        assertEquals( 2L, resultDelete.getTotalCount() );
        assertEquals( Arrays.asList(  "a", "a.b", "a.b.e", "a.b.e.f", "a.b.e.i" ), carbonjAdmin.findAllMetrics( "a"));
    }

    @Test
    public void deleteWithExcludeNonLeafTest() throws IOException {
        cjClient.send( "a.b.e.f.h", 1.0f, new DateTime() );
        cjClient.send( "a.b.x.y.z", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.l.m.n", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("a.b", true, Arrays.asList("f", "l"));
        assertEquals( 1L, resultDelete.getLeafCount());
        assertEquals( 3L, resultDelete.getTotalCount() );
        assertEquals( Arrays.asList(  "a", "a.b", "a.b.e", "a.b.e.f", "a.b.e.f.h", "a.b.c", "a.b.c.l", "a.b.c.l.m", "a.b.c.l.m.n"  ), carbonjAdmin.findAllMetrics( "a") );
    }

    @Test
    public void deleteWithNoTrueDeleteTest() throws IOException {
        cjClient.send( "a.b.e.f.h", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.l.m", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("a.b", false, Arrays.asList("h"));
        assertEquals( 1L, resultDelete.getLeafCount() );
        assertEquals( 3L, resultDelete.getTotalCount() );
        assertEquals( Arrays.asList(  "a", "a.b", "a.b.e", "a.b.e.f", "a.b.e.f.h", "a.b.c", "a.b.c.l", "a.b.c.l.m"), carbonjAdmin.findAllMetrics( "a") );
    }

    @Test
    public void deleteWithSuffixTest() throws IOException {
        cjClient.send( "a.b.c.g.k.l", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.k.n", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.k.o", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.f.c.g.k", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.f.c.g.m.c.g.k", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.f.c.g.c.g.k.f", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("**.c.g.k", true, Collections.EMPTY_LIST );

        assertEquals( 6L, resultDelete.getLeafCount() );
        assertEquals( 8L, resultDelete.getTotalCount() );

        assertEquals( Arrays.asList(  "a", "a.b", "a.b.c", "a.b.c.g", "a.b.c.g.f", "a.b.c.g.f.c", "a.b.c.g.f.c.g", "a.b.c.g.f.c.g.m", "a.b.c.g.f.c.g.m.c", "a.b.c.g.f.c.g.m.c.g", "a.b.c.g.f.c.g.c", "a.b.c.g.f.c.g.c.g"), carbonjAdmin.findAllMetrics( "a") );
    }

    @Test
    public void deleteWithSuffixAndExclude() throws IOException {
        cjClient.send( "a.b.c.g.k.l", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.k.n", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.k.o", 1.0f, new DateTime() );
        cjClient.send( "a.b.c.g.f.c.g.k.f", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("**.c.g.k", true, Arrays.asList("l", "o", "f"));

        assertEquals( 1L, resultDelete.getLeafCount() );
        assertEquals( 1L, resultDelete.getTotalCount() );

        assertEquals( Arrays.asList(  "a", "a.b", "a.b.c", "a.b.c.g", "a.b.c.g.k", "a.b.c.g.k.l", "a.b.c.g.k.o", "a.b.c.g.f", "a.b.c.g.f.c", "a.b.c.g.f.c.g", "a.b.c.g.f.c.g.k", "a.b.c.g.f.c.g.k.f"), carbonjAdmin.findAllMetrics( "a") );
    }

    @Test
    public void deleteWithWildCardTest() throws IOException {
        cjClient.send( "abc42.def.ghi.klm.nop", 1.0f, new DateTime() );
        cjClient.send( "abc53.def.ghi.klm.nop", 1.0f, new DateTime() );
        cjClient.send( "abc73.def.ghi.klm.nop", 1.0f, new DateTime() );
        cjClient.send( "abc102.def.ghi.klm.nop", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("abc*.*.*.*.*", true, Collections.EMPTY_LIST);

        assertEquals( 4L, resultDelete.getLeafCount() );
        assertEquals( 4L, resultDelete.getTotalCount() );

        assertEquals( Arrays.asList( "abc42", "abc42.def", "abc42.def.ghi", "abc42.def.ghi.klm"), carbonjAdmin.findAllMetrics( "abc42") );
        assertEquals( Arrays.asList( "abc53", "abc53.def", "abc53.def.ghi", "abc53.def.ghi.klm"), carbonjAdmin.findAllMetrics( "abc53") );
        assertEquals( Arrays.asList( "abc73", "abc73.def", "abc73.def.ghi", "abc73.def.ghi.klm"), carbonjAdmin.findAllMetrics( "abc73") );
        assertEquals( Arrays.asList( "abc102", "abc102.def", "abc102.def.ghi", "abc102.def.ghi.klm"), carbonjAdmin.findAllMetrics( "abc102") );
    }

    @Test
    public void deleteWithWildCardAndExcludeTest() throws IOException {
        cjClient.send( "abc42.def.ghi.klm.nop1", 1.0f, new DateTime() );
        cjClient.send( "abc53.def.ghi.klm.nop2", 1.0f, new DateTime() );
        cjClient.send( "abc73.def.ghi.klm.nop3", 1.0f, new DateTime() );
        cjClient.send( "abc102.def.ghi.klm.nop4", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("abc*.*.*.klm", true, Arrays.asList("nop1"));

        assertEquals( 3L, resultDelete.getLeafCount() );
        assertEquals( 6L, resultDelete.getTotalCount() );

        assertEquals( Arrays.asList( "abc42", "abc42.def", "abc42.def.ghi", "abc42.def.ghi.klm","abc42.def.ghi.klm.nop1"), carbonjAdmin.findAllMetrics( "abc42") );
        assertEquals( Arrays.asList( "abc53", "abc53.def", "abc53.def.ghi"), carbonjAdmin.findAllMetrics( "abc53") );
        assertEquals( Arrays.asList( "abc73", "abc73.def", "abc73.def.ghi"), carbonjAdmin.findAllMetrics( "abc73") );
        assertEquals( Arrays.asList( "abc102", "abc102.def", "abc102.def.ghi"), carbonjAdmin.findAllMetrics( "abc102") );
    }

    @Test
    public void deleteAndRemoveChildTest() throws IOException {
        cjClient.send( "a.b.e.f.h", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.f.g", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.x.m.n", 1.0f, new DateTime() );
        cjClient.send( "a.b.e.y.m.n", 1.0f, new DateTime() );
        drain();

        DeleteAPIResult resultDelete = carbonjAdmin.deleteAPI("a.b.e.f", true, Collections.EMPTY_LIST);
        assertEquals( 2L, resultDelete.getLeafCount());
        assertEquals( 3L, resultDelete.getTotalCount() );
        Metric m = metricIndex.getMetric("a.b.e");
        Assert.assertArrayEquals(Arrays.asList("x","y").toArray(),m.children().toArray() );
        assertEquals( Arrays.asList(  "a", "a.b", "a.b.e",  "a.b.e.x", "a.b.e.x.m", "a.b.e.x.m.n", "a.b.e.y", "a.b.e.y.m", "a.b.e.y.m.n"  ), carbonjAdmin.findAllMetrics( "a") );
    }
}
