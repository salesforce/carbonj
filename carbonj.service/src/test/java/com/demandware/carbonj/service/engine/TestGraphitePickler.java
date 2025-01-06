/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.RetentionPolicy;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGraphitePickler {
    @Test
    public void testPickler() throws Exception {
        GraphitePickler pickler = new GraphitePickler();
        pickler = new GraphitePickler(true);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        List<Metric> nodes = new ArrayList<>();
        nodes.add(new Metric("foo.bar", 1, null, List.of(RetentionPolicy.getInstance("60s:24h")), new ArrayList<>()));
        pickler.pickleMetrics(nodes, baos);
        assertEquals(93, baos.toByteArray().length);
    }
}
