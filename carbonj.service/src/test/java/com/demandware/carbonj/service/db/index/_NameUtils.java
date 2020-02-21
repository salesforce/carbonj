/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.Test;

public class _NameUtils
{
    NameUtils utils = new NameUtils( "root" );

    @Test
    public void testIsValid()
    {
        // testing empty case
        assertThat( utils.isValid( "" ), equalTo( false ) );

        // testing root rules
        assertThat( utils.isValid( "root" ), equalTo( true ) );
        assertThat( utils.isValid( "root.a.b.c" ), equalTo( false) );

        // testing [0-9a-zA-Z] case
        assertThat( utils.isValid( "abc.123.Z9x" ), equalTo( true ) );

        // testing [.] rules
        assertThat( utils.isValid( ".aaa" ), equalTo( false ) );
        assertThat( utils.isValid( "abc..123" ), equalTo( false ) );
        assertThat( utils.isValid( "abc.123." ), equalTo( false ) );

        // testing special characters
        assertThat( utils.isValid("_-:.=.%" ), equalTo( true ) );
        assertThat( utils.isValid("&^" ), equalTo( false ) );
        assertThat( utils.isValid("!@.#.$" ), equalTo( false ) );
    }

    @Test
    public void testFirstSegmentForNameWithMultipleSegments()
    {
        assertThat( utils.firstSegment( "aaaa.b.ccc" ), equalTo("aaaa") );
    }

    @Test
    public void testFirstSegmentForNameWithSingleSegment()
    {
        assertThat( utils.firstSegment( "a" ), equalTo( "a" ) );
    }

    @Test
    public void testIsTopLevel()
    {
        assertThat( utils.isTopLevel( "abc" ), equalTo( true ) );
        assertThat( utils.isTopLevel( "a.b" ), equalTo( false ) );
    }

    @Test
    public void testParentName()
    {
        assertThat( utils.parentName( "abc" ), equalTo( Optional.empty() ) );
        assertThat( utils.parentName( "a.b" ), equalTo( Optional.of( "a" ) ) );
        assertThat( utils.parentName( "a.b.c" ), equalTo( Optional.of( "a.b" ) ) );
    }
}
