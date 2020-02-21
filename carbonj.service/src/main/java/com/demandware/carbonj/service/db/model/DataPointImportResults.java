/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

public class DataPointImportResults
{
    public final String dbName;
    public final int received;
    public final int saved;
    public final int errors;
    public final int expired;

    public DataPointImportResults(String dbName, int received, int saved, int errors, int expired)
    {
        this.dbName = dbName;
        this.received = received;
        this.saved = saved;
        this.errors = errors;
        this.expired = expired;
    }
}
