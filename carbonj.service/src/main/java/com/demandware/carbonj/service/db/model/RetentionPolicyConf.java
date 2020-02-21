/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;

public class RetentionPolicyConf
{
    /**
     * Provides retention policies defined for a given metric.
     *
     * @param metricName name of the metric.
     * @return list of retention policies ordered by resolution. Highest resolution to Lowest.
     */
    public List<RetentionPolicy> getRetentionPolicies( String metricName )
    {
        return RetentionPolicy.getPolicyList( "60s:24h,5m:7d,30m:2y" );
    }

}
