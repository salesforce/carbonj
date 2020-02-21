/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.externalsorting.ExternalSort;

/**
 * Implementation of staging file sort using java sort library.
 */
class JavaSort implements StagingFilesSort
{
    private static final Logger log = LoggerFactory.getLogger(JavaSort.class);

    @Override
    public void sort( File inFile, Optional<File> extraMergeFile, File outFile)
        throws IOException
    {
        log.debug(String.format("sorting [%s], with mergeFile [%s], into [%s]", inFile, extraMergeFile.orElse( null ), outFile));
        List<File> list = ExternalSort.sortInBatch( inFile );
        extraMergeFile.ifPresent( list::add );
        ExternalSort.mergeSortedFiles( list, outFile );
    }

}
