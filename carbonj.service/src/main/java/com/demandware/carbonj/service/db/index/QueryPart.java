/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

public class QueryPart {

    private final String query;

    private final boolean isRegEx;

    QueryPart(String query, boolean isRegEx) {
        this.query = query;
        this.isRegEx = isRegEx;
    }

    public String getQuery() {
        return query;
    }

    public boolean isRegEx() {
        return isRegEx;
    }
}
