/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class _QueryUtils
{
    @Test
    public void patternToRegEx()
    {
        assertEquals("^abc(.*prd|.*stg|.*)xyz$", QueryUtils.patternToRegEx( "abc{*prd,*stg,*}xyz" ));
    }

    @Test
    public void matchPatternWithList()
    {
        String query = "abc-{*prd,*stg,*_tst}-xyz";

        List<String> entries = Arrays.asList("abc-AAA_stg-xyz", "abc-AAA_mmm-xyz", "abc-AAA_prd-xyz", "abc-AAA_tst-xyz" );
        List<String> expected = Arrays.asList("abc-AAA_stg-xyz", "abc-AAA_prd-xyz", "abc-AAA_tst-xyz");

        String[] patterns = QueryUtils.splitQuery(query);
        assertEquals(expected, QueryUtils.filter( entries, patterns[0] ));
    }
}
