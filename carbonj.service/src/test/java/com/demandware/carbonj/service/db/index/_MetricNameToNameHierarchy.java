/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith( Parameterized.class)
public class _MetricNameToNameHierarchy
{
    @Parameterized.Parameter(0)
    public String name;

    @Parameterized.Parameter(1)
    public String[] names;

    @Test
    public void isValid()
    {
        assertThat( new NameUtils("root").metricNameHierarchy( name ), equalTo(names));
    }

    @Parameterized.Parameters(name= "{index}: metricNameHierarchy({0})={1}")
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
