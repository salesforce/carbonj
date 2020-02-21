/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.IOException;

import com.demandware.carbonj.service.db.model.Series;

public interface ResponseStream
{
    /**
     * Implementation for opening the stream should be thread-safe.
     */
    void openSeriesList() throws IOException;

    /**
     * Implementation for writing to the stream should be thread-safe.
     */
    void writeSeries( Series s ) throws IOException;

    /**
     * Implementation for closing stream should be thread-safe.
     */
    void closeSeriesList() throws IOException;

    /**
     * Closes the stream. should be thread-safe.
     */
    void close() throws IOException;
}
