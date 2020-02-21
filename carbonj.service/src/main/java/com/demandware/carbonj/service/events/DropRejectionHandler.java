/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import com.demandware.carbonj.service.engine.RejectionHandler;

import java.util.concurrent.BlockingQueue;

public class DropRejectionHandler<T> implements RejectionHandler<T> {

    @Override
    public void rejected(BlockingQueue<T> queue, T o) {

    }
}
