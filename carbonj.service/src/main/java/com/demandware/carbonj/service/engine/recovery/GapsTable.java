/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import java.util.List;

public interface GapsTable {

    void add(Gap gap) throws Exception;

    List<Gap> getGaps() throws Exception;

    void delete(Gap gap) throws Exception;

    boolean updateGap(Gap gap) throws Exception;

    void destroy();
}
