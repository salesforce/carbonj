/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteAPIResult
{
    private long responseTime;
    private int totalCount;
    private int leafCount;
    private boolean success;
    private String error;
    private List<String> metricsList = new ArrayList<>();

    public long getResponseTtime()
    {
        return this.responseTime;
    }

    public DeleteAPIResult setResponseTtime( long responseTime )
    {
        this.responseTime = responseTime;
        return this;
    }

    public int getTotalCount()
    {
        return this.totalCount;
    }

    public DeleteAPIResult setTotalCount( int totalCount )
    {
        this.totalCount = totalCount;
        return this;
    }

    public int getLeafCount()
    {
        return this.leafCount;
    }

    public DeleteAPIResult setLeafCount( int leafCount )
    {
        this.leafCount = leafCount;
        return this;
    }

    public boolean getSuccess()
    {
        return this.success;
    }

    public DeleteAPIResult setSuccess( boolean success )
    {
        this.success = success;
        return this;
    }

    public String getError()
    {
        return this.error;
    }

    public DeleteAPIResult setError( String error )
    {
        this.error = error;
        return this;
    }

    public List<String> getMetricsList()
    {
        return this.metricsList;
    }

    public DeleteAPIResult setMetricsList( List<String> metricsList )
    {
        if( metricsList != null && !metricsList.isEmpty() )
        {
            this.metricsList.addAll(metricsList);
        }
        return this;
    }

}
