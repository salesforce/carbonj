/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;

import org.junit.After;
import org.junit.Before;

import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.io.Files;

public class BaseIndexTest
{
    File dbDirFile;
    MetricIndex index;
    NameUtils nameUtils;

    @Before
    public void setUp()
    {
        nameUtils = new NameUtils(InternalConfig.getRootEntryKey());
        dbDirFile = Files.createTempDir();
        index = IndexUtils.metricIndex( dbDirFile );
        index.open();
    }

    @After
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
