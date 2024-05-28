/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

public class StagingFileRecord
{
    public final StagingFileSet fileName;
    public final long metricId;
    public final String strValue;

    StagingFileRecord( StagingFileSet fileName, long metricId, String strValue)
    {
        this.fileName = fileName;
        this.metricId = metricId;
        this.strValue = strValue;
    }

    StagingFileRecord( StagingFileSet fileName, String line)
    {
        this.fileName = fileName;

        //TODO: verify that this logic is faster than line.split(" ")
        int idStart = 0;
        int idEnd = line.indexOf( ' ' );

        int valueStart = idEnd + 1;

        metricId = Long.parseLong( line.substring( idStart, idEnd ) );
        strValue = line.substring( valueStart );
    }

    double val()
    {
        return Double.parseDouble( strValue );
    }

    @Override
    public String toString()
    {
        return "StagingFileRecord{" +
                        "fileName=" + fileName +
                        ", metricId=" + metricId +
                        ", strValue='" + strValue + '\'' +
                        '}';
    }
}
