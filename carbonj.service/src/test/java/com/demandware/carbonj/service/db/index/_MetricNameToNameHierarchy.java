/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class _MetricNameToNameHierarchy
{
    @ParameterizedTest
    @MethodSource("data")
    public void isValid(String name, String[] names)
    {
        assertArrayEquals( new NameUtils("root").metricNameHierarchy( name ), names);
    }

    public static Iterable<Object[]> data()
    {
        return Arrays.asList(new Object[][]
                        {
                            {"a", new String[] {"a"}},
                            {"a.bb", new String[] {"a", "a.bb"} },
                            {"aaa.b.cc", new String[] {"aaa", "aaa.b", "aaa.b.cc"}},
                            {"*", new String[] {"*" }},
                            {"*.*.*", new String[] { "*", "*.*", "*.*.*"}},
                            {"*.pod2[0-9].x", new String[] {"*", "*.pod2[0-9]", "*.pod2[0-9].x"} },
                            {"*.{pod1,pod2,pod3}.*", new String[] { "*", "*.{pod1,pod2,pod3}", "*.{pod1,pod2,pod3}.*"}}
                        });
    }
}
