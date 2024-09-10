/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class BaseIndexTest
{
    File dbDirFile;
    MetricIndex index;
    NameUtils nameUtils;

    @BeforeEach
    public void setUp()
    {
        nameUtils = new NameUtils(InternalConfig.getRootEntryKey());
        dbDirFile = Files.createTempDir();
        index = IndexUtils.metricIndex( dbDirFile, false );
        index.open();
    }

    @AfterEach
    public void tearDown()
    {
        if( index != null )
        {
            index.close();
        }

        if( dbDirFile != null )
        {
            dbDirFile.delete();
        }
    }

    Metric findOrCreate( String name)
    {
        return IndexUtils.findOrCreate( index, name );
    }

}
