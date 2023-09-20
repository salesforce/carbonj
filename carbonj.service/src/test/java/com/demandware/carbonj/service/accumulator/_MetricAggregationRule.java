/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import static com.demandware.carbonj.service.accumulator.MetricAggregationMethod.LATENCY;
import static com.demandware.carbonj.service.accumulator.MetricAggregationMethod.SUM;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

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
        ecom_agRule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag.<realm>.<tenant>.<metric> (60) = sum <pod>.ecom.<realm>.<tenant>.*.*.<<metric>>" );
        ocapiRule  = MetricAggregationRule.parseDefinition( "ocapi.<metric> (60) drop = sum pod[0-9]{1,2}.ecom.*.*.*.*.ocapi.<<metric>>" );
        ecom_agLatencyRule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag_latency.infrastructure.metrics.blade.reachability (60) = latency <pod>.infrastructure.metrics.blade.*.*.reachability" );

        ecom_agRequestRule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag.<realm>.<tenant>.requests.active-requests.count (60) c = sum <pod>.ecom.<realm>.<tenant>.*.*.requests.*.active-requests.count" );
        ecom_agRequest_new_Rule =  MetricAggregationRule.parseDefinition( "<pod>.ecom_ag.<realm>.<tenant>.requests.active-requests.count (60) c = sum <pod>.ecom.<realm>.<tenant>.*.*.requests.((?!on-request)(*)).active-requests.count" );
    }

    @Test
    public void shouldApply_ecom_ag_request_rule()
    {
        String name1 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.on-request.active-requests.count";
        String name2 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.pipeline.active-requests.count";
        Result expected1 = new Result("pod1.ecom_ag.aaba.aaba_prd.requests.active-requests.count", SUM, false );
        Result expected2 = new Result("pod1.ecom_ag.aaba.aaba_prd.requests.active-requests.count", SUM, false );
        assertThat( ecom_agRequestRule.apply( name1 ), equalTo( expected1 ) );
        assertThat( ecom_agRequestRule.apply( name2 ), equalTo( expected2 ) );
    }

    @Test
    public void shouldApply_ecom_ag_request_new_rule()
    {
        String name1 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.on-request.active-requests.count";
        String name2 = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.requests.pipeline.active-requests.count";
        Result expected1 = new Result(null, null, false ); // Should drop on-request
        Result expected2 = new Result("pod1.ecom_ag.aaba.aaba_prd.requests.active-requests.count", SUM, false );
        assertThat( ecom_agRequest_new_Rule.apply( name1 ), equalTo( expected1 ) );
        assertThat( ecom_agRequest_new_Rule.apply( name2 ), equalTo( expected2 ) );
    }
    @Test
    public void shouldApply_ecom_ag_rule()
    {
        String name = "pod1.ecom.aaba.aaba_prd.blade5-4.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled";
        Result
            expected = new Result("pod1.ecom_ag.aaba.aaba_prd.FEATURE_TOGGLES.KeystoreAutoImportEnabled", SUM, false );
        assertThat( ecom_agRule.apply( name ), equalTo( expected ) );
    }

    @Test
    public void shouldApply_ecom_ag_latency_rule()
    {
        String name = "pod5.infrastructure.metrics.blade.application.blade7-2.reachability";
        Result
                expected = new Result("pod5.ecom_ag_latency.infrastructure.metrics.blade.reachability", LATENCY, false );
        assertThat( ecom_agLatencyRule.apply( name ), equalTo( expected ) );
    }

    @Test
    public void shouldApply_ocapi_rule()
    {
        String name = "pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.ocapi.apis.categories.sizes.response.count";
        Result expected = new Result("ocapi.apis.categories.sizes.response.count", SUM, true );
        assertThat( ocapiRule.apply( name ), equalTo( expected ) );
    }

    @Test
    public void noMatchWhenDifferentPrefix()
    {
        String name = "pi.pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.ocapi.apis.categories.sizes.response.count";
        Result expected = new Result(null, null, false );
        assertThat( ecom_agRule.apply( name ), equalTo( expected ) );
    }

    @Test
    public void noMatchWhenDifferentPos()
    {
        String name = "pod41.ecom.abbm.abbm_stg.blade0-0.abbm_stg.NOTOCAPI.ocapi.apis.categories.sizes.response.count";
        Result expected = new Result(null, null, false );
        assertThat( ocapiRule.apply( name ), equalTo( expected ) );
    }

}
