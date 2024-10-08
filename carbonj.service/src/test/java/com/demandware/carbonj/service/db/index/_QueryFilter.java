/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class _QueryFilter
{
    @Test
    public void selectWithStartInOneLevel()
    {
        QueryPart[] queries = QueryUtils.splitQuery( "*" );
        List<String> candidates = Arrays.asList("aaa", "bbb", "c", "d");
        List<String> expected = Arrays.asList( "aaa", "bbb", "c", "d" );
        assertEquals( QueryUtils.filter( candidates, queries[0], null), expected );
    }

    @Test
    public void selectWithExactMatch()
    {
        QueryPart[] queries = QueryUtils.splitQuery( "bbb" );
        List<String> candidates = Arrays.asList("aaa", "bbb", "c", "d");
        List<String> expected = List.of("bbb");
        assertEquals( QueryUtils.filter( candidates, queries[0], null), expected );
    }

    @Test
    public void selectWithNoMatch()
    {
        QueryPart[] queries = QueryUtils.splitQuery( "x*" );
        List<String> candidates = Arrays.asList("aaa", "bbb", "c", "d");
        List<String> expected = Collections.emptyList();
        assertEquals( QueryUtils.filter( candidates, queries[0], null), expected );
    }


    @Test
    public void selectMatch()
    {
        QueryPart[] queries = QueryUtils.splitQuery( "a*a" );
        List<String> candidates = Arrays.asList("aaa", "bbb", "c", "d", "adkdddda", "ajjaj");
        List<String> expected = Arrays.asList( "aaa", "adkdddda" );
        assertEquals( QueryUtils.filter( candidates, queries[0], null), expected );
    }
}
