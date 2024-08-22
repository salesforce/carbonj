/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.File;

import com.demandware.carbonj.service.util.TestFileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class _StorageAggregationRules
{
    File configFile;

    StorageAggregationRules rules;


    @BeforeEach
    public void setUp()
        throws Exception
    {
        configFile = TestFileUtils.setupTestFileFromResource( "/storage-aggregation.conf" );
        rules = new StorageAggregationRulesLoader( configFile ).getRules();
    }


    @Test
    public void shouldApply()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled.count";
        assertEquals( rules.apply( name ), AggregationMethod.SUM );
    }

    @Test
    public void shouldApply2()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled";
        assertEquals( rules.apply( name ), AggregationMethod.AVG );
    }

}
