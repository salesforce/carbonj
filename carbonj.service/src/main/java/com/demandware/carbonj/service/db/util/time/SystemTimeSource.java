/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.util.time;

class SystemTimeSource implements TimeSource
{
    @Override
    public int getEpochSecond()
    {
        return (int)(System.currentTimeMillis() / 1000);
    }
}
