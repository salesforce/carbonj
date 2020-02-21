/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;

public class DataPointExportResults
{
    public final String dbName;

    public final String metricName;

    public final List<DataPointValue> values;

    public DataPointExportResults( String dbName, String metricName, List<DataPointValue> values )
    {
        this.dbName = dbName;
        this.metricName = metricName;
        this.values = values;
    }

}
