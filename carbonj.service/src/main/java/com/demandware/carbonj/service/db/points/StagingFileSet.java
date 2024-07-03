/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.demandware.carbonj.service.db.util.time.TimeSource;

/**
 * Represents group of file names with data points for a specific interval within data point archive (retention policy)
 * <p>
 * Unsorted file names: "<dbName>-<ts>.<seq>"
 * Sorted file names: "<dbName>-<ts>.<seq>.s"
 * <p>
 * All data points within file have the same ts (interval).
 * <p>
 * File content:
 * <p>
 * <metricId> <value>
 * <p>
 * If new data points arrive while the file is being sorted/processed file with the new sequence is started. At a later
 * time this new sequence file will be sorted along with the previous sorted file and processed.
 */
public class StagingFileSet
{

    public static final String PARTS_DELIMITER = "-";

    public static final String SORTED_EXT = ".s";

    /**
     * Unique id for this staging file set.
     */
    final String id;

    /**
     * Data point archive name (database name).
     */
    final String dbName;

    /**
     * Interval that all values in this file set belong to.
     */
    final int from;

    final int group;

    final private int hashCode;

    private int collectWhenUnmodifiedFor = 600;

    TimeSource timeSource = TimeSource.defaultTimeSource();

    StagingFileSet(File stagingFile)
    {
        this(stagingFile.getName().substring(0, stagingFile.getName().indexOf( '.' )));
    }

    public StagingFileSet( String id )
    {
        this.id = id;
        String[] elements = id.split(PARTS_DELIMITER);
        dbName = elements[0];
        from = Integer.parseInt(elements[1]);
        group = elements.length > 2 ? Integer.parseInt(elements[2]) : Integer.MAX_VALUE;
        this.collectWhenUnmodifiedFor = getCollectionIntervalSeconds( dbName );
        this.hashCode = calcHashCode();
    }

    public StagingFileSet( String dbName, int from) {
        this(dbName, from, Integer.MAX_VALUE);
    }

    public StagingFileSet( String dbName, int from, int group) {
        this.id = getId(dbName, from, group);
        this.dbName = dbName;
        this.from = from;
        this.group = group;
        this.collectWhenUnmodifiedFor = getCollectionIntervalSeconds( dbName );
        this.hashCode = calcHashCode();
    }

    public static String getId(String dbName, int from, int group) {
        return dbName + PARTS_DELIMITER + from + PARTS_DELIMITER + group;
    }

    private int getCollectionIntervalSeconds(String dbName)
    {
        return switch (dbName) {
            case "5m7d" -> 90; // 90 sec
            case "30m2y" -> 5 * 60; // 5 min
            default -> 20 * 60; // 20 min
        };
    }

    public boolean needsCollection(int lastModified)
    {
        int now = timeSource.getEpochSecond();
        return now > lastModified + this.collectWhenUnmodifiedFor;
    }

    public String getNextUnsortedFileName(File dir)
    {
        int nextSeq = lastUnsorted( dir ) + 1;
        return nameForUnsorted(id, nextSeq);
    }

    public String nameForUnsorted(String id, int seq)
    {
        return id + "." + seq;
    }

    private String nameForSorted(String id, int seq)
    {
        return nameForUnsorted( id, seq ) + SORTED_EXT;
    }

    public Optional<String> getLastSortedFileName(File dir)
    {
        int lastSeq = lastSorted( dir );
        return lastSeq > 0  ? Optional.of( nameForSorted( id, lastSeq ) ) : Optional.empty();
    }

    public int lastUnsorted(File dir)
    {
        List<Integer> used =  existingFiles( dir, id, false );
        return used.stream().max( Integer::compare ).orElse( 0 );
    }

    public int lastSorted(File dir)
    {
        List<Integer> used =  existingFiles( dir, id, true );
        return used.stream().max( Integer::compare ).orElse( 0 );
    }

    private List<Integer> existingFiles( File dir, String fileId, boolean sorted)
    {
        List<Integer> seqs = new ArrayList<>();
        try(DirectoryStream<Path> paths = Files.newDirectoryStream( dir.toPath() ))
        {
            paths.forEach( p -> extractSequence( p, fileId, sorted ).ifPresent(seqs::add));
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
        seqs.sort(Integer::compare);
        return seqs;
    }

    private Optional<Integer> extractSequence( Path path, String fileId, boolean sorted)
    {
        String name = path.getFileName().toString();
        int dot = name.indexOf( '.' );

        // ignore unrelated file
        if (dot <= 0 || !name.substring(0, dot).equals( fileId ))
        {
            return Optional.empty();
        }

        // "<file-id>.<seq>" for unsorted files or "<file-id>.<seq>.s" for sorted files
        int seqStart = dot + 1;
        String seq;
        if( sorted )
        {
            int end = name.indexOf( ".s", seqStart );
            if( end > 0  )
            {
                seq = name.substring( seqStart, end );
            }
            else
            {
                return Optional.empty();
            }
        }
        else
        {
            seq = name.substring( seqStart );
        }
        try
        {
            return Optional.of(Integer.parseInt( seq ));
        }
        catch(NumberFormatException e)
        {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
            return true;
        if ( !(o instanceof StagingFileSet fileName) )
            return false;

        if ( from != fileName.from )
            return false;
        return id.equals( fileName.id );

    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    private int calcHashCode()
    {
        int result = id.hashCode();
        result = 31 * result + from;
        return result;
    }

    public static Set<StagingFileSet> allStagingFileNamesIn( File dir)
    {
        Set<StagingFileSet> names = new HashSet<>();
        try
        {
            try(DirectoryStream<Path> paths = Files.newDirectoryStream( dir.toPath() ))
            {
                paths.forEach( p -> StagingFileSet.forPath( p )
                                                  .ifPresent(names::add) );
            }
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
        return names;
    }


    public static Optional<StagingFileSet> forPath( Path path)
    {
        try
        {
            return Optional.of(new StagingFileSet( path.toFile() ));
        }
        catch(Exception e)
        {
            return Optional.empty();
        }
    }

    @Override
    public String toString()
    {
        return "StagingFileSet{" +
                        "id='" + id + '\'' +
                        '}';
    }
}
