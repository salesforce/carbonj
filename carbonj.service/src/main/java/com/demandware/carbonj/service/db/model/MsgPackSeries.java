/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.model;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;


/**
 * Represents series data to be sent to graphite.
 */
public class MsgPackSeries
{
    @JsonProperty("start")
    final public long start;
    @JsonProperty("end")
    final public long end;
    @JsonProperty("step")
    final public long step;
    @JsonProperty("name")
    final public String name;
    @JsonProperty("pathExpression")
    final public String pathExpression;
    @JsonProperty("values")
    final public List<Double> values;

    public MsgPackSeries( Series series)
    {
        this.start = series.start;
        this.end = series.end;
        this.step = series.step;
        this.name = series.name;
        this.pathExpression = series.name;
        this.values = series.values;
    }

    @JsonCreator
    public MsgPackSeries( @JsonProperty( "start" ) long start, @JsonProperty( "end" ) long end,
                          @JsonProperty( "step" ) long step, @JsonProperty( "name" ) String name,
                          @JsonProperty( "pathExpression" ) String pathExpression,
                          @JsonProperty( "values" ) List<Double> values )
    {
        this.start = start;
        this.end = end;
        this.step = step;
        this.name = name;
        this.pathExpression = pathExpression;
        this.values = values;
    }

    @Override
    public String toString()
    {
        return "MsgPackSeries{" +
                        "start=" + start +
                        ", end=" + end +
                        ", step=" + step +
                        ", name=" + name +
                        ", pathExpression=" + pathExpression +
                        ", values=" + values +
                        '}';
    }
}