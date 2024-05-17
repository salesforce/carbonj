/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.model.MetricProvider;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central service for managing staging files.
 */
public class StagingFiles
{
    private static final Logger log = LoggerFactory.getLogger(StagingFiles.class);

    private final MetricRegistry metricRegistry;

    /**
     * Tracks current open files.
     */
    private volatile ConcurrentMap<StagingFileSet, StagingFile> files = new ConcurrentHashMap<>();

    private final File dir;

    private final StagingFileSetCollector fileSetCollector;

    private final StagingFilesSort sort;

    private final MetricProvider metricProvider;

    StagingFiles(MetricRegistry metricRegistry, File dir, StagingFilesSort sort, MetricProvider metricProvider)
    {
        this.metricRegistry = metricRegistry;
        this.dir = Preconditions.checkNotNull( dir );
        this.fileSetCollector = new StagingFileSetCollector( dir );
        this.sort = Preconditions.checkNotNull( sort );
        this.metricProvider = Preconditions.checkNotNull( metricProvider );
    }

    @PostConstruct
    public void init()
    {
        // on start load files that haven't been processed yet
        StagingFileSet.allStagingFileNamesIn( dir ).forEach( this::openIfIncomplete );
    }

    private void openIfIncomplete(StagingFileSet fn)
    {
        int lastUnsortedSeq = fn.lastUnsorted( dir );
        if( fn.lastSorted( dir ) < lastUnsortedSeq )
        {
            log.info("found incomplete staging file: [" + fn.id + "]. Adding to list of open files.");
            files.put( fn, reopen(fn, lastUnsortedSeq) );
        }
    }

    private File[] files()
    {
        File[] files = dir.listFiles();
        if( files == null )
        {
            files = new File[0];
        }
        return files;
    }

    public void deleteFilesOlderThan(int time)
    {
        log.info("time: " + time);
        for(File f : files())
        {
            long lastModified = f.lastModified();
            log.info( "File: [" + f + "] last modified: " + lastModified );
            int lastModifiedInSeconds = Math.toIntExact( lastModified / 1000 );
            if( f.isFile() && lastModifiedInSeconds  < time)
            {
                if( files.containsValue( new StagingFile( metricRegistry, f, sort, metricProvider ) ) )
                {
                    log.info("Skipping removal of file: [" + f + "] because it is still in use.");
                }
                else
                {
                    if ( !f.delete() )
                    {
                        log.error( "Failed to delete file: [" + f + "]" );
                    }
                    else
                    {
                        log.info( "Deleted file: [" + f + "]" );
                    }
                }
            }
            else
            {
                log.info( "Skipping removal of file: [" + f + "]");
            }
        }
    }

    public List<SortedStagingFile> collectEligibleFiles(String dbName)
    {
        return fileSetCollector.collectEligibleFiles(files, dbName);
    }

    public void write(StagingFileRecord r)
    {

        StagingFile f = files.get( r.fileName );
        if( f == null )
        {
            // only one thread writes to files but propagating/sorting thread can close the file.
            f = open(r.fileName);
            files.put( r.fileName, f );
        }


        if( !f.write( r ) ) // file was closed
        {
            log.info( "first write failed. opening next sequence file " + r.fileName );
            f = open(r.fileName); // open next file
            files.put( r.fileName, f );
            if( !f.write( r ))
            {
                log.error( "Unexpected state - the file was closed. file: [" + r.fileName + "]. Skipping this data point: " + r);
            }
        }
    }

    private StagingFile reopen( StagingFileSet fs, int seq)
    {
        File f = new File( dir, fs.nameForUnsorted(fs.id, seq) );
        Preconditions.checkState( f.exists() );
        StagingFile sf = new StagingFile(metricRegistry, f, sort, metricProvider);
        sf.open();
        return sf;
    }

    private StagingFile open(StagingFileSet fs)
    {
        File f = new File( dir, fs.getNextUnsortedFileName( dir ) );
        Preconditions.checkState( !f.exists() );
        StagingFile sf = new StagingFile(metricRegistry, f, sort, metricProvider);
        sf.open();
        return sf;
    }

    public void flush()
    {
        files.values().forEach(StagingFile::flush);
    }

    public void close()
    {
        files.values().forEach(StagingFile::close);
    }
}
