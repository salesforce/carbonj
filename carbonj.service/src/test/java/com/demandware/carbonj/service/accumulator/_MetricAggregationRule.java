/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import static com.demandware.carbonj.service.accumulator.MetricAggregationMethod.LATENCY;
import static com.demandware.carbonj.service.accumulator.MetricAggregationMethod.SUM;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.demandware.carbonj.service.accumulator.MetricAggregationRule.Result;

public class _MetricAggregationRule
{
    private MetricAggregationRule ecom_agRule;
    private MetricAggregationRule ocapiRule;
    private MetricAggregationRule ecom_agLatencyRule;
    private MetricAggregationRule ecom_agRequestRule;
    private MetricAggregationRule ecom_agRequest_new_Rule;


    @Before
    public void setUp()
    {
        ecom_agRule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag.<realm>.<tenant>.<metric> (60) = sum <pod>.ecom.<realm>.<tenant>.*.*.<<metric>>", 0, true );
        ocapiRule  = MetricAggregationRule.parseDefinition( "ocapi.<metric> (60) drop = sum pod[0-9]{1,2}.ecom.*.*.*.*.ocapi.<<metric>>", 1, true );
        ecom_agLatencyRule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag_latency.infrastructure.metrics.blade.reachability (60) = latency <pod>.infrastructure.metrics.blade.*.*.reachability", 2, true );

        ecom_agRequestRule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag.<realm>.<tenant>.requests.active-requests.count (60) c = sum <pod>.ecom.<realm>.<tenant>.*.*.requests.*.active-requests.count", 3, true );
        ecom_agRequest_new_Rule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag.<realm>.<tenant>.requests.active-requests.count (60) c = sum <pod>.ecom.<realm>.<tenant>.*.*.requests.((?!onrequest)(*)).active-requests.count", 4, true );
    }

    @Test
    public void shouldApply_ecom_ag_request_rule()
    {
        String name1 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.onrequest.active-requests.count";
        String name2 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.pipeline.active-requests.count";
        Result expected1 = new Result("pod1.ecom_ag.aaba.aaba_prd.requests.active-requests.count", SUM, false );
        Result expected2 = new Result("pod1.ecom_ag.aaba.aaba_prd.requests.active-requests.count", SUM, false );
        assertEquals( ecom_agRequestRule.apply( name1 ), expected1 );
        assertEquals( ecom_agRequestRule.apply( name2 ), expected2 );
    }

    @Test
    public void shouldApply_ecom_ag_request_new_rule()
    {
        String name1 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.onrequest.active-requests.count";
        String name2 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.pipeline.active-requests.count";
        Result expected1 = new Result(null, null, false ); // Should drop onrequest
        Result expected2 = new Result("pod1.ecom_ag.aaba.aaba_prd.requests.active-requests.count", SUM, false );
        assertEquals( ecom_agRequest_new_Rule.apply( name1 ), expected1 );
        assertEquals( ecom_agRequest_new_Rule.apply( name2 ), expected2 );
    }
    @Test
    public void shouldApply_ecom_ag_rule()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled";
        Result
            expected = new Result("pod1.ecom_ag.aaba.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled", SUM, false );
        assertEquals( ecom_agRule.apply( name ), expected );
    }

    @Test
    public void shouldApply_ecom_ag_latency_rule()
    {
        String name = "pod5.infrastructure.metrics.blade.application.blade7-2.reachability";
        Result
                expected = new Result("pod5.ecom_ag_latency.infrastructure.metrics.blade.reachability", LATENCY, false );
        assertEquals( ecom_agLatencyRule.apply( name ), expected );
    }

    @Test
    public void shouldApply_ocapi_rule()
    {
        String name = "pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.ocapi.apis.categories.sizes.response.count";
        Result expected = new Result("ocapi.apis.categories.sizes.response.count", SUM, true );
        assertEquals( ocapiRule.apply( name ), expected );
    }

    @Test
    public void noMatchWhenDifferentPrefix()
    {
        String name = "pi.pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.ocapi.apis.categories.sizes.response.count";
        Result expected = new Result(null, null, false );
        assertEquals( ecom_agRule.apply( name ), expected );
    }

    @Test
    public void noMatchWhenDifferentPos()
    {
        String name = "pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.NOTOCAPI.ocapi.apis.categories.sizes.response.count";
        Result expected = new Result(null, null, false );
        assertEquals( ocapiRule.apply( name ), expected );
    }

}
