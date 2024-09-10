/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;

public class AbstractCarbonJBaseTest
{
    private static File serviceDirStatic;

    @Value( "${app.servicedir:}" ) private String serviceDir;

    @PostConstruct public void prepareServiceDir()
    {
        if ( !serviceDir.isEmpty() )
        {
            serviceDirStatic = new File( serviceDir );
        }
        else
        {
            serviceDirStatic = new File( System.getProperty( "user.dir" ) );
        }
    }

    @AfterAll
    public static void cleanup()
                    throws IOException
    {
        try
        {
            FileUtils.deleteDirectory( new File( serviceDirStatic, "work/carbonj-data" ) );
            FileUtils.deleteDirectory( new File( serviceDirStatic, "work/carbonj-staging" ) );
            FileUtils.deleteDirectory( new File( serviceDirStatic, "work/carbonj-checkpoint" ) );
        }
        catch ( Exception e )
        {
            // ignore
        }
    }

    @BeforeAll
    public static void init()
                    throws IOException
    {
        new File( serviceDirStatic, "work/carbonj-data" ).mkdirs();
        new File( serviceDirStatic, "work/carbonj-staging" ).mkdirs();
        new File( serviceDirStatic, "work/carbonj-checkpoint" ).mkdirs();
    }
}
