/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.util.TestFileUtils;
import org.junit.runner.RunWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

public class _StorageAggregationRules
{
    File configFile;

    StorageAggregationRules rules;


    @Before
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
        assertThat( rules.apply( name ), equalTo( AggregationMethod.SUM ) );
    }

    @Test
    public void shouldApply2()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled";
        assertThat( rules.apply( name ), equalTo( AggregationMethod.AVG ) );
    }

}
