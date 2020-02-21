/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import java.util.Collection;

public interface PayloadV2Codec {

    byte[] encode(Collection<byte[]> eventCollections) throws Exception;
    Collection<byte[]> decode(byte[] encodedBytes) throws Exception;
}
