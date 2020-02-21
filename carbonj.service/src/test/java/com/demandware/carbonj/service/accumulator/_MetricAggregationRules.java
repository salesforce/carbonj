/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import static com.demandware.carbonj.service.accumulator.MetricAggregationMethod.SUM;
import static com.demandware.carbonj.service.accumulator.MetricAggregationMethod.AVG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.accumulator.MetricAggregationRule.Result;
import com.demandware.carbonj.service.util.TestFileUtils;

public class _MetricAggregationRules
{
    File configFile;

    MetricAggregationRules rules;


    @Before
    public void setUp()
        throws Exception
    {
        configFile = TestFileUtils.setupTestFileFromResource( "/aggregation-rules-test.conf" );
        MetricAggregationRulesLoader rulesLoader = new MetricAggregationRulesLoader( configFile );
        rules = rulesLoader.getRules();
    }


    @Test
    public void shouldApply_ecom_ag_rule_with_sum()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled";
        List<Result> expected = Arrays.asList(new Result("pod1.ecom_ag.aaba.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled",
            SUM, false ));
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

    @Test
    public void shouldApply_ecom_ag_rule_with_avg()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled.mean";
        List<Result> expected = Arrays.asList( new Result("pod1.ecom_ag.aaba.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled.mean",
            AVG, false ));
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

    @Test
    public void shouldApply_ocapi_rule()
    {
        String name = "pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.ocapi.apis.categories.sizes.response.count";
        List<Result> expected = Arrays.asList( new Result("ocapi.apis.categories.sizes.response.count", SUM,
            true ));
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

    @Test
    public void noRuleApplied()
    {
        String name = "abc.pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.ocapi.apis.categories.sizes.response.count";
        List<Result> expected = Collections.EMPTY_LIST;
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

    @Test
    public void multipleRulesApplied()
    {
        String name = "pi.abc.response.count";
        List<Result> expected = Arrays.asList(
            new Result("pi.subgroup.response.count", AVG, false ),
            new Result("pi.response.count", AVG, true ));
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

    @Test
    public void multipleRulesAppliedWithDrop()
    {
        String name = "pi.xyz.response.count";
        List<Result> expected = Arrays.asList(
            new Result("pi.subgroup2.response.count", AVG, true ),
            new Result("pi.response.count", AVG, true ));
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

    @Test
    public void matchOnOnlyOneFromMultipleRules()
    {
        String name = "pi.ttt.response.count";
        List<Result> expected = Arrays.asList( new Result("pi.response.count", AVG, true ) );
        assertThat( rules.apply( name ), equalTo( expected ) );
    }

}
