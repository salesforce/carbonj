/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class _NameUtils
{
    NameUtils utils = new NameUtils( "root" );

    @Test
    public void testIsValid()
    {
        // testing empty case
        assertFalse( utils.isValid( "" ) );

        // testing root rules
        assertTrue( utils.isValid( "root" ) );
        assertFalse( utils.isValid( "root.a.b.c" ) );

        // testing [0-9a-zA-Z] case
        assertTrue( utils.isValid( "abc.123.Z9x" ) );

        // testing [.] rules
        assertFalse( utils.isValid( ".aaa" ) );
        assertFalse( utils.isValid( "abc..123" ) );
        assertFalse( utils.isValid( "abc.123." ) );

        // testing special characters
        assertTrue( utils.isValid("_-:.=.%" ) );
        assertFalse( utils.isValid("&^" ) );
        assertFalse( utils.isValid("!@.#.$" ) );
    }

    @Test
    public void testFirstSegmentForNameWithMultipleSegments()
    {
        assertEquals( utils.firstSegment( "aaaa.b.ccc" ), "aaaa" );
    }

    @Test
    public void testFirstSegmentForNameWithSingleSegment()
    {
        assertEquals( utils.firstSegment( "a" ), "a" );
    }

    @Test
    public void testIsTopLevel()
    {
        assertTrue( utils.isTopLevel( "abc" ) );
        assertFalse( utils.isTopLevel( "a.b" ) );
    }

    @Test
    public void testParentName()
    {
        assertEquals( utils.parentName( "abc" ), Optional.empty() );
        assertEquals( utils.parentName( "a.b" ), Optional.of( "a" ) );
        assertEquals( utils.parentName( "a.b.c" ), Optional.of( "a.b" ) );
    }
}
