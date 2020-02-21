/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util;

import java.io.File;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public class FileUtils
{
    public static File writableDir(String path)
    {
        Preconditions.checkState( StringUtils.isNotEmpty(path), "Dir path is empty" );
        File dir = new File( path );
        Preconditions.checkState( dir.exists(), "Dir [" + dir.getAbsolutePath() + "] doesnt' exist.");
        Preconditions.checkState( dir.canRead(), "Do not have permissions to read from " + dir.getAbsolutePath() );
        Preconditions.checkState( dir.canWrite(), "Do not have permissions to write to " + dir.getAbsolutePath() );
        return dir;
    }

}
