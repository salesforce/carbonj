/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.points;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestStagingFileRecord {
    @Test
    public void test() {
        StagingFileSet stagingFileSet = new StagingFileSet(new File("/tmp/5m7d-1734989700-9.1.s"));
        String line = "11987976699 2 pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io";
        StagingFileRecord stagingFileRecord = new StagingFileRecord(stagingFileSet, line);
        assertEquals(2, stagingFileRecord.val());
        assertEquals(11987976699L, stagingFileRecord.metricId);
        assertEquals("pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io", stagingFileRecord.metricName);
        assertEquals("StagingFileRecord{fileName=StagingFileSet{id='5m7d-1734989700-9'}, metricId=11987976699, strValue='2', metricName=pod276.oracle_db_server.database.crs07.blade5-6.bkvk.bkvk2.OPS_USER.6y9rrcvjksgum.851311265.io}", stagingFileRecord.toString());
    }
}
