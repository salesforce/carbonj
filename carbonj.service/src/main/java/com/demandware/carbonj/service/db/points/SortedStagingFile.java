/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import com.demandware.carbonj.service.db.model.IntervalValues;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Provides access to a sorted staging file - loads data points in sets by metric id.
 */
class SortedStagingFile
{
    private static final Logger log = LoggerFactory.getLogger(SortedStagingFile.class);

    private StagingFileSet fileName;
    private File file;
    private MetricProvider metricProvider;

    private BufferedReader br;

    private StagingFileRecord returnedRecord;

    SortedStagingFile( File file, MetricProvider metricProvider )
    {
        this.file = Preconditions.checkNotNull(file);
        this.fileName = new StagingFileSet( file );
        this.metricProvider = Preconditions.checkNotNull( metricProvider );
    }

    public String dbName()
    {
        return fileName.dbName;
    }

    public void open()
    {
        returnedRecord = null;
        try
        {
            this.br = new BufferedReader( new FileReader( file ) );
        }
        catch(IOException e)
        {
            Throwables.propagate( e );
        }
    }

    private String nextLine()
    {
        try
        {
            return br.readLine();
        }
        catch(IOException e)
        {
            throw new UncheckedIOException( e );
        }
    }

    private StagingFileRecord nextRecord()
    {
        StagingFileRecord r = null;
        if( returnedRecord != null )
        {
            r = returnedRecord;
            returnedRecord = null;
        }
        else
        {
            String line = nextLine();
            if( line != null )
            {
                r = new StagingFileRecord( fileName, line );
            }
        }
        return r;
    }

    private void putBack(StagingFileRecord r)
    {
        Preconditions.checkState( returnedRecord == null );
        returnedRecord = r;
    }

    public void close()
    {
        IOUtils.closeQuietly(br);
        br = null;
    }

    public boolean isClosed()
    {
        return br == null;
    }

    public Optional<IntervalValues> loadNeighbours()
    {
        StagingFileRecord r = nextRecord();
        if ( r == null )
        {
            return Optional.empty();
        }

        long metricId = r.metricId;
        List<Double> vals = new ArrayList<>();

        while ( true )
        {
            if ( r == null )
            {
                // reached end of file
                break;
            }

            if ( r.metricId != metricId )
            {   // read all points that belong to current metric
                putBack( r );
                break;
            }

            vals.add( r.val() );
            r = nextRecord();
        }

        Metric m = metricProvider.forId( metricId );
        if( m == null )
        {
            throw new RuntimeException(String.format("Failed to find metric with metricId [%s].", metricId));
        }
        return Optional.of( new IntervalValues( m, vals, fileName.from, fileName.dbName ) );
    }

    @Override
    public String toString()
    {
        return "SortedStagingFile{" +
                        "file=" + file +
                        '}';
    }
}
