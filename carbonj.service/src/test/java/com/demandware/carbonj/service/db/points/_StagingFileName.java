/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import com.demandware.carbonj.service.BaseTest;
import org.junit.Ignore;
import org.junit.Test;

import com.demandware.carbonj.service.db.model.MetricProvider;
import com.google.common.io.Files;

public class _StagingFileName extends BaseTest
{
    @Test
    public void testGetNextUnsortedFileName()
                    throws IOException
    {
        StagingFileSet fn = new StagingFileSet( new File("5m7d-100000.x") );
        File dir = Files.createTempDir();
        assertEquals( Optional.empty(), fn.getLastSortedFileName( dir ));
        assertEquals( "5m7d-100000.1", fn.getNextUnsortedFileName( dir ));
        assertEquals( "5m7d-100000.1", fn.getNextUnsortedFileName( dir ));
        File x = new File(dir, "5m7d-100000.1");
        assertTrue(x.createNewFile());
        assertEquals( "5m7d-100000.2", fn.getNextUnsortedFileName( dir ));
        File x2 = new File(dir, "5m7d-100000.2");
        assertTrue(x2.createNewFile());
        assertEquals( "5m7d-100000.3", fn.getNextUnsortedFileName( dir ));
    }

    // TODO fails on Jenkins for no apparent reason with
    // _StagingFileName.testGetLastSortedFileName:49 expected:<Optional[5m7d-100000.1.s]> but was:<Optional.empty>
    @Test
    @Ignore
    public void testGetLastSortedFileName()
                    throws IOException
    {
        StagingFileSet fn = new StagingFileSet( new File("5m7d-100000.x") );
        File dir = Files.createTempDir();
        assertEquals( Optional.empty(), fn.getLastSortedFileName( dir ));
        assertEquals( "5m7d-100000.1", fn.getNextUnsortedFileName( dir ));
        File x = new File(dir, "5m7d-100000.1");
        assertTrue(x.createNewFile());
        StagingFile stagingFile = new StagingFile(metricRegistry, x, sort(), mock( MetricProvider.class) );
        stagingFile.sort( Optional.empty() );

        assertEquals( Optional.of("5m7d-100000.1.s"), fn.getLastSortedFileName( dir ));
        assertEquals( "5m7d-100000.2", fn.getNextUnsortedFileName( dir ));
        File x2 = new File(dir, "5m7d-100000.2");
        assertTrue(x2.createNewFile());
        StagingFile stagingFile2 = new StagingFile( metricRegistry, x2, sort(), mock( MetricProvider.class) );
        stagingFile2.sort( Optional.of("5m7d-100000.1.s") );
        assertEquals( Optional.of("5m7d-100000.2.s"), fn.getLastSortedFileName( dir ));
    }

    private StagingFilesSort sort()
    {
        SystemSort s = new SystemSort(  );
        s.setParallel( -1 );
        return s;
    }

    @Test
    public void canParseUnsortedFileId()
    {
        StagingFileSet fn = new StagingFileSet( new File("5m7d-100000.1"));
        assertAttributes( fn, "5m7d-100000", "5m7d", 100000);
    }

    @Test
    public void canParseSortedFileId()
    {
        StagingFileSet fn = new StagingFileSet( new File("5m7d-100000.1.s"));
        assertAttributes( fn, "5m7d-100000", "5m7d", 100000);
   }

    @Test
    public void canBuildFileId()
    {
        StagingFileSet fn = new StagingFileSet( "5m7d", 100000);
        assertAttributes( fn, "5m7d-100000", "5m7d", 100000);
    }

    private void assertAttributes( StagingFileSet fn, String fileId, String dbName, int from)
    {
        assertEquals( fileId, fn.id);
        assertEquals( dbName, fn.dbName );
        assertEquals( from, fn.from);
    }

}
