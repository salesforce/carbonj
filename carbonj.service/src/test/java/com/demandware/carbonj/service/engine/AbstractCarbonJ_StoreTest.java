/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.admin.CarbonjAdmin;
import com.demandware.carbonj.service.db.model.MetricIndex;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractCarbonJ_StoreTest
                extends AbstractCarbonJSvcTest
{
    @Autowired protected PointFilter pointFilter;

    @Autowired protected CarbonjAdmin carbonjAdmin;

    @Autowired protected MetricIndex metricIndex;

    protected static final String DB_60S = "60s24h";

    protected static final String DB_5M = "5m7d";

    protected static final String DB_30M = "30m2y";

    @BeforeEach
    public void before()
    {
        dropData();
    }

    protected void dropData()
    {
        timeSeriesStore.deleteAll();
        pointFilter.reset();
    }
}
