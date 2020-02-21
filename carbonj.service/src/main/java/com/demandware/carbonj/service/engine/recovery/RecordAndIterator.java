/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.amazonaws.services.kinesis.model.Record;

class RecordAndIterator {

    static final RecordAndIterator EMPTY = new RecordAndIterator(null, null);

    private Record record;
    private String iterator;


    RecordAndIterator(Record record, String iterator) {
        this.record = record;
        this.iterator = iterator;
    }

    public Record getRecord() {
        return record;
    }

    public String getIterator() {
        return iterator;
    }
}
