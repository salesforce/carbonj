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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class _MetricNameValidator
{
    @ParameterizedTest
    @MethodSource("data")
    public void isValid(String name, boolean expected)
    {
        assertEquals( new NameUtils("root").isValid( name ), expected);
    }

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
