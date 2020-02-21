/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

interface StagingFilesSort
{
    void sort( File inFile, Optional<File> extraMergeFile, File outFile) throws IOException;
}
