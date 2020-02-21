/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import java.util.Date;
import java.util.Objects;

public class GapImpl implements Gap {

    private final Date startTime;
    private final Date endTime;
    private final Date lastRecovered;

    public GapImpl(Date startTime, Date endTime) {
        this(startTime, endTime, startTime);
    }

    GapImpl(Date startTime, Date endTime, Date lastRecovered) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.lastRecovered = lastRecovered;
    }

    @Override
    public Date startTime() {
        return startTime;
    }

    @Override
    public Date endTime() {
        return endTime;
    }

    @Override
    public Date lastRecovered() {
        return lastRecovered;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GapImpl gap = (GapImpl) o;
        return Objects.equals(startTime, gap.startTime) &&
                Objects.equals(endTime, gap.endTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime);
    }

    @Override
    public int compareTo(Gap o) {
        return this.startTime.compareTo(o.startTime());
    }

    @Override
    public String toString() {
        return "<" + startTime + " - " + endTime + ">";
    }
}
