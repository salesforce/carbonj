/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@RunWith( Parameterized.class)
public class _MetricNameValidator
{
    @Parameter(0)
    public String name;

    @Parameter(1)
    public boolean expected;

    @Test
    public void isValid()
    {
        assertThat( new NameUtils("root").isValid( name ), is(expected));
    }

    @Parameters(name= "{index}: isValid({0})={1}")
    public static Iterable<Object[]> data()
    {
        return Arrays.asList(new Object[][]
        {
                        {"a", true},
                        {"a.b", true},
                        {"a.b.c", true},
                        {"a.root", true},
                        {null, false},
                        {"", false},
                        {" ", false},
                        {" a", false},
                        {"a ", false},
                        {".", false},
                        {".a", false},
                        {"a.", false},
                        {"root", true},
                        {"root.a", false},
                        {"rootA.b", true},
                        {"a.b^", false},
                        {"a.b ", false},
                        {" a.b", false},
                        {"abcdefghijklmnopqrstuvwxyz.ABCDEFGHIJKLMNOPQRSTUVWXYZ.0123456789.:=-_%", true},
                        {"a.{", false},
                        {"a.}", false},
                        {"a.(", false},
                        {"a.)", false},
                        {"a.!", false},
                        {"a.@", false},
                        {"a.#", false},
                        {"a.$", false},
                        {"a.%", true},
                        {"a.^", false},
                        {"a.&", false},
                        {"a.*", false},
                        {"a.<", false},
                        {"a.>", false},
                        {"a.,", false},
                        {"a.?", false},
                        {"a./", false},
                        {"a.'", false},
                        {"a.\"", false},
                        {"a.;", false},
                        {"a.[", false},
                        {"a.]", false},
                        {"a.+", false},
                        {"a.~", false},
                        {"a.`", false},
                        {"a.|", false},
                        {"a.\\", false}

        });
    }
}
