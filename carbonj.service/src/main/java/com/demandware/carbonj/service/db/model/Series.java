/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;

/**
 * Represents series data to be sent to graphite.
 */
public class Series
{
    final public String name;
    final public long start;
    final public long end;
    final public long step;
    final public List<Double> values;

    //TODO: replace with double[]
    public Series( String name, long start, long end, long step, List<Double> values)
    {
        this.name = name;
        this.start = start;
        this.end = end;
        this.step = step;
        this.values = values;
    }

    @Override
    public String toString()
    {
        return "Series{" +
                        "name='" + name + '\'' +
                        ", start=" + start +
                        ", end=" + end +
                        ", step=" + step +
                        ", values=" + values +
                        '}';
    }
}

/*
Query:
        curl "http://refapp:8080/render/?format=pickle&target=carbon.agents.refapp_mon_demandware_net-a.creates&from=1451448900&until=1451448997" > fetch.out

Response:
might have multiple series:
        [{
            'name': 'carbon.agents.refapp_mon_demandware_net-a.creates',
            'start': 1451448960,
            'end': 1451449020,
            'step': 60,
            'values': [0.0]}]
*/
