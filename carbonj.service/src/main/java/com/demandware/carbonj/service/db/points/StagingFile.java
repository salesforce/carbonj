/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.db.model.MetricProvider;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingFile implements Closeable
{
    private static final Logger log = LoggerFactory.getLogger(StagingFile.class);

    private final File file;

    private BufferedWriter bw;

    private final StagingFilesSort sort;

    private final MetricProvider metricProvider;

    private static final ConcurrentMap<String, Timer> stagingFileSortTimerMap = new ConcurrentHashMap<>();

    StagingFile(MetricRegistry metricRegistry, File file, StagingFilesSort sort, MetricProvider metricProvider) {
        this(metricRegistry, file, sort, metricProvider, "");
    }

    StagingFile(MetricRegistry metricRegistry, File file, StagingFilesSort sort, MetricProvider metricProvider, String dbName)
    {
        this.file = file;
        this.sort = sort;
        this.metricProvider = Preconditions.checkNotNull( metricProvider );
        if (!stagingFileSortTimerMap.containsKey(dbName)) {
            String timerName = "staging.filesort.timer";
            if (!StringUtils.isEmpty(dbName)) {
                timerName = timerName + "." + dbName;
            }
            stagingFileSortTimerMap.putIfAbsent(dbName, metricRegistry.timer(timerName));
        }
    }

    public synchronized int lastModified()
    {
        return Math.toIntExact( file.lastModified() / 1000 );
    }

    public synchronized void open()
    {
        //Should handle cases when file already exists.
        try
        {
            this.bw = new BufferedWriter( new OutputStreamWriter( new FileOutputStream( file, true ) ) );
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public synchronized boolean write( StagingFileRecord r )
    {
        if( isClosed() )
        {
            return false;
        }

        try
        {
            bw.append( String.valueOf( r.metricId ) );
            bw.write( " " );
            bw.write( r.strValue );
            bw.write( " " );
            bw.write( r.metricName );
            bw.write( "\n");
            return true;
        }
        catch(IOException e)
        {
            throw new UncheckedIOException( e );
        }
    }

    public synchronized void flush()
    {
        try
        {
            if( !isClosed() )
            {
                bw.flush();
            }
        }
        catch(IOException e)
        {
            throw new UncheckedIOException( e );
        }
    }

    public synchronized void close()
    {
        log.info("closing staging file [" + file + "]");
        if( bw != null )
        {
            try
            {
                bw.close();
            }
            catch(Exception e)
            {
                log.error( "Error when closing file [" + file + "]", e );
            }
            finally
            {
                bw = null;
            }
        }
    }

    public synchronized boolean isClosed()
    {
        return bw == null;
    }

    /**
     * Sorts this file and returns new file with sorted data.
     *
     * @param lastSorted additional sorted file that will be included in merge step. This file will be removed as part
     *                   of merge.
     * @return file with sorted data. Original file will be unchanged.
     */
    public synchronized SortedStagingFile sort( Optional<String> lastSorted, String dbName)
    {
        Preconditions.checkState( isClosed(), "Staging file must be closed before sorting." );

        File dir = file.getParentFile();
        Optional<File> extraMergeFile = lastSorted.map( n -> new File(dir, n) );
        //TODO: need to make sure we won't attempt to reopen this staging file while sorting is in progress.
        File outFile = new File(dir, file.getName() + ".s");
        if( outFile.exists() )
        {
            log.info( "Sorted file already exists. Wait for it to be processed. [" + outFile + "]" );
            return null;
        }

        sort(file, extraMergeFile, outFile, dbName);
        return new SortedStagingFile( outFile, metricProvider );
    }



    private void sort(File inFile, Optional<File> extraMergeFile, File outFile, String dbName)
    {
        try (Timer.Context ignored = stagingFileSortTimerMap.get(dbName).time())
        {
            sort.sort( inFile, extraMergeFile, outFile );
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        return "StagingFile{" +
                        "file=" + file +
                        '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !(o instanceof StagingFile that) )
            return false;

        return file.equals( that.file );

    }

    @Override
    public int hashCode()
    {
        return file.hashCode();
    }
}
