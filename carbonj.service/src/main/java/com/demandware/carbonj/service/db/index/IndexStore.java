/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import java.io.File;
import java.io.PrintWriter;
import java.util.function.Consumer;

public interface IndexStore<K, R extends Record<K>>
{
    R dbGet( K key );

    void dbDelete( K key );

    void dbPut( R e );

    String getName();

    File getDbDir();

    void open();

    void close();

    void dumpStats();

    void dump( PrintWriter pw );

    String dbGetProperty(String property);

    K maxKey();

    long scan( K startKey, K endKey, Consumer<R> c );
}
