/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Implementation of staging file sort using linux sort command.
 */
class SystemSort implements StagingFilesSort
{
    private static final Logger log = LoggerFactory.getLogger(SystemSort.class);

    private static final long DEFAULT_TIMEOUT_IN_SECONDS = 300;

    private final long timeoutInSeconds;

    private long bufSizeKb = 1048576; // 1GiB
    private String tmpDir = "/tmp";
    private int parallel = 2;

    SystemSort()
    {
        this(DEFAULT_TIMEOUT_IN_SECONDS);
    }

    SystemSort(long timeoutInSeconds)
    {
        Preconditions.checkArgument( timeoutInSeconds > 0 );
        this.timeoutInSeconds = timeoutInSeconds;
    }

    String[] sortArgs(File in, File out)
    {
        List<String> args = new ArrayList<>(  );
        args.add( "sort" );
        args.add( "-sn" );
        args.add( "-k1,1" );
        addTmpDirArg( args );
        addBufSizeArg( args );
        addParallelArg( args );
        args.add( in.getAbsolutePath() );
        args.add( "-o" );
        args.add( out.getAbsolutePath() );

        return args.toArray(new String[args.size()]);
    }

    String[] mergeArgs(File in1, File in2, File out)
    {
        List<String> args = new ArrayList<>(  );
        args.add( "sort" );
        args.add( "-snm" );
        args.add( "-k1,1" );
        addTmpDirArg( args );
        addBufSizeArg( args );
        addParallelArg( args );
        args.add( in1.getAbsolutePath() );
        args.add( in2.getAbsolutePath() );
        args.add( "-o" );
        args.add( out.getAbsolutePath() );


        return args.toArray(new String[args.size()]);
    }

    private void addParallelArg( List<String> args )
    {
        if( parallel > 0 )
        {
            log.debug( String.format( "--parallel option is set to [%s]", parallel) );
            args.add( "--parallel" );
            args.add( String.valueOf( parallel ) );
        }
        else
        {
            log.debug( "--parallel option is not set." );
        }
    }

    private void addBufSizeArg( List<String> args )
    {
        if( bufSizeKb > 0 )
        {
            log.debug( String.format( "--buffer-size is set to [%s]", bufSizeKb ) );
            args.add( "--buffer-size" );
            args.add( String.valueOf( bufSizeKb ) + "K" );
        }
        else
        {
            log.debug( "--buffer-size is not set." );
        }
    }

    private void addTmpDirArg( List<String> args )
    {
        if( tmpDir != null && tmpDir.length() > 0 )
        {
            log.debug( String.format("--temporary-directory is set to [%s]", tmpDir) );
            File tmpDirFile = new File(tmpDir);
            Preconditions.checkState( tmpDirFile.exists(), String.format( "Temp dir [%s] doesn't exist", tmpDir ) );
            Preconditions.checkState( tmpDirFile.canRead(), String.format( "Do not have read permissions in temp dir [%s]", tmpDir ) );
            Preconditions.checkState( tmpDirFile.canWrite(), String.format( "Do not have write permissions in temp dir [%s]", tmpDir ) );
            args.add( "--temporary-directory" );
            args.add( tmpDir );
        }
        else
        {
            log.debug( "--temporary-directory is not set." );
        }
    }

    @Override
    public void sort( File inFile, Optional<File> extraMergeFile, File outFile) throws IOException
    {
        log.debug(String.format("sorting [%s], with mergeFile [%s], into [%s]", inFile, extraMergeFile.orElse( null ), outFile));
        File tmpOutFile = outFile;
        if( extraMergeFile.isPresent() )
        {
            tmpOutFile = new File(outFile.getParent(), outFile.getName() + ".tmp");
        }

        exec(sortArgs(inFile, tmpOutFile) );

        if( extraMergeFile.isPresent())
        {
            File mergeFile = extraMergeFile.get();
            exec(mergeArgs( tmpOutFile, mergeFile, outFile) );
            tmpOutFile.delete();
        }
    }

    private void exec(String[] args) throws IOException
    {
        List<String> stdout = new ArrayList<>();
        Process process = new ProcessBuilder(args).redirectErrorStream(true).start();
        readIO(process.getInputStream(), stdout);
        try
        {
            process.waitFor( timeoutInSeconds, TimeUnit.SECONDS );
        }
        catch(InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        int exitValue = process.exitValue();
        if( exitValue != 0 )
        {
            log.error( String.format( "sort exit value [%s], stdout of sort %s is: [%s] ",
                exitValue, Arrays.toString( args ), stdout ) );
        }
    }

    private void readIO(final InputStream src, final List<String> output) {
        new Thread( () -> {
            Scanner sc = new Scanner(src);
            while (sc.hasNextLine()) {
                output.add( sc.nextLine());
            }
        } ).start();
    }

    public void setBufSizeKb( long bufSizeKb )
    {
        this.bufSizeKb = bufSizeKb;
    }

    public void setTmpDir( String tmpDir )
    {
        this.tmpDir = tmpDir;
    }

    public void setParallel( int parallel )
    {
        this.parallel = parallel;
    }
}
