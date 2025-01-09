/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import com.demandware.carbonj.service.engine.AggregationMethod;
import com.demandware.carbonj.service.engine.StorageAggregationRulesLoader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStorageAggregationPolicySource {
    @Test
    public void testStorageAggregationPolicySource() {
        String metricName = "pod276.ecom.bbdl.bbdl_prd.blade6-4.bbdl_prd.ocapi.clients.99c874ec-90ef-42fa-bf08-4becc2893202.apis.shop.versions.v20_2.methods.post.resources.customers_auth.requests.m15_rate";
        File storageConfFile = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource("storage-aggregation.conf")).getFile());
        StorageAggregationRulesLoader storageAggregationRulesLoader = new StorageAggregationRulesLoader(storageConfFile);
        StorageAggregationPolicySource storageAggregationPolicySource = new StorageAggregationPolicySource(storageAggregationRulesLoader);
        assertEquals(1, storageAggregationPolicySource.currentConfigRevision());
        AggregationPolicy aggregationPolicy = storageAggregationPolicySource.policyForMetricName(metricName);
        assertEquals(AggregationMethod.AVG, aggregationPolicy.getMethod());
        storageAggregationPolicySource.cleanup();
    }
}
