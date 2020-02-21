/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.config;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigUtils
{
    public static File locateConfigFile(String appRoot, String fileName )
    {
        File f = new File( fileName );
        if ( f.isAbsolute() )
        {
            return f;
        }

        f = new File( appRoot, fileName );
        if ( !f.exists() )
        {
            // check current working dir (running from IDE in dev environment)
            File cwdFile = new File( fileName );
            if ( cwdFile.exists() ) // keep fs.config location if the file doesn't exist in current working dir.
            {
                f = cwdFile;
            }
        }
        return f;
    }

    /**
     * Load lines from file. Trims whitespace, removes comments and empty lines.
     */
    public static List<String> lines(File f)
    {
        try
        {
            return FileUtils.readLines( f ).stream()
                            .map( line -> line.trim() )
                            .filter( line -> line.length() != 0 && !line.startsWith( "#" ) )
                            .collect( Collectors.toList() );
        }
        catch(IOException e)
        {
            throw new UncheckedIOException( e );
        }
    }


}
