/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import org.apache.commons.io.IOUtils;

public class TestFileUtils
{
    public static File setupTestFileFromResource( String testResourceName)
        throws IOException
    {
        File tmpFile = Files.createTempFile( TestFileUtils.class.getCanonicalName(), "test-file" ).toFile();
        setupTestFileFromResource( testResourceName, tmpFile );
        return tmpFile;
    }

    public static void setupTestFileFromResource( String testResourceName, File testFile )
    {
        InputStream in = null;
        OutputStream out = null;
        try
        {
            in = TestFileUtils.class.getResourceAsStream( testResourceName );
            out = new FileOutputStream( testFile );
            IOUtils.copy( in, out );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            IOUtils.closeQuietly( in );
            IOUtils.closeQuietly( out );
        }

    }

}
