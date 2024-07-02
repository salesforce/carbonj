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
    public final String metricName;

    StagingFileRecord( StagingFileSet fileName, long metricId, String strValue, String metricName)
    {
        this.fileName = fileName;
        this.metricId = metricId;
        this.strValue = strValue;
        this.metricName = metricName;
    }

    StagingFileRecord( StagingFileSet fileName, String line)
    {
        this.fileName = fileName;

        //TODO: verify that this logic is faster than line.split(" ")
        int idStart = 0;
        int idEnd = line.indexOf( ' ' );

        metricId = Long.parseLong( line.substring( idStart, idEnd ) );
        idStart = idEnd + 1;
        idEnd = line.indexOf(' ', idStart);
        if (idEnd < 0) {
            strValue = line.substring(idStart);
            metricName = null;
        } else {
            strValue = line.substring(idStart, idEnd);
            metricName = line.substring(idEnd + 1);
        }
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
                        ", metricName=" + metricName +
                        '}';
    }
}
