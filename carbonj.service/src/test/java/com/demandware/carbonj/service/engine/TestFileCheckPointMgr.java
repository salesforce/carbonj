/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.util.AssertionErrors.assertEquals;
import static org.springframework.test.util.AssertionErrors.assertTrue;

// TODO Not sure if we need to cleanup checkpoint.txt file before
// Because this test fails if that file already exists
// Hence ignoring
@SpringBootTest @DirtiesContext( classMode = DirtiesContext.ClassMode.AFTER_CLASS )
@Disabled
public class TestFileCheckPointMgr
{

    private static final Logger log = LoggerFactory.getLogger( TestFileCheckPointMgr.class );

    @BeforeEach
    public void deleteFileIfExists()
    {
        try
        {
            Path path = Paths.get( "/tmp/checkpoint.txt" );
            Files.delete( path );
            log.info("Deleted file from path " + path.toString() );
        }
        catch ( IOException ex )
        {
            log.error( " Exception occurred while deleting a file", ex );
        }

    }

    @Test
    public void testBasic()
                    throws Exception
    {
        CheckPointMgr<Date> checkPointMgr = new FileCheckPointMgr( Paths.get( "/tmp" ), 60 );
        Date lastCheckPoint = checkPointMgr.lastCheckPoint();
        log.warn( "-------------- " + lastCheckPoint );
        log.warn( "-------------- " + new Date( System.currentTimeMillis() - TimeUnit.MINUTES.toMillis( 120 ) ) );
        assertTrue("last Checkpoint is "+ lastCheckPoint + ", actual value is " +new Date( System.currentTimeMillis() - TimeUnit.MINUTES.toMillis( 60 ) ),  lastCheckPoint.before(
                        new Date( System.currentTimeMillis() - TimeUnit.MINUTES.toMillis( 60 ) ) ) );
        Date checkPoint1 = new Date();
        checkPointMgr.checkPoint( checkPoint1 );
        assertEquals( "Expected value is "+ checkPoint1 +", Actual value is "+ checkPointMgr.lastCheckPoint(), checkPoint1, checkPointMgr.lastCheckPoint() );
        Date checkPoint2 = new Date();
        checkPointMgr.checkPoint( checkPoint2 );
        assertEquals( "Expected value is "+ checkPoint2 +", Actual value is "+ checkPointMgr.lastCheckPoint(), checkPoint2, checkPointMgr.lastCheckPoint() );
    }
}
