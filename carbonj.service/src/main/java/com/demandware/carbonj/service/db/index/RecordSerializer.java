/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

interface RecordSerializer<K, R>
{
    R toIndexEntry(byte[] keyBytes, byte[] valueBytes);
    R toIndexEntry(K key, byte[] valueBytes);
    byte[] keyBytes(K key);
    byte[] valueBytes(R e);
    K key(byte[] keyBytes);
}
