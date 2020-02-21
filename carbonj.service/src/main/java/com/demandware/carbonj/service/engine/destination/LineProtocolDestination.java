/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.demandware.carbonj.service.engine.DataPoint;

import java.util.function.Consumer;

public interface LineProtocolDestination
    extends Consumer<DataPoint>
{
    void closeQuietly();
}
