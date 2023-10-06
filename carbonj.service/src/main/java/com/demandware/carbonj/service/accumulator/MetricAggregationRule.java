/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.demandware.carbonj.service.strings.StringsCache;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.text.StrSubstitutor;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

class MetricAggregationRule
{
    private final int id;

    final private String inputPattern;
    final private String outputPattern;
    final MetricAggregationMethod method;

    final private String outputTemplate;
    final private Pattern pattern;
    final private List<String> fieldNames = new ArrayList<>(  );

    final private boolean dropOriginal;

    // do not check any other rules after this one has matched.
    final private boolean stopRule;

    private final boolean aggregationRuleCacheEnabled;

    public static class Result
    {
        final String aggregateName;
        final MetricAggregationMethod method;
        final boolean dropOriginal;

        Result(String aggregateName, MetricAggregationMethod method, boolean dropOriginal)
        {
            this.aggregateName = aggregateName;
            this.method = aggregateName != null ? method : null; //TODO...
            this.dropOriginal = aggregateName != null && dropOriginal;
        }

        public String getAggregateName()
        {
            return aggregateName;
        }

        public MetricAggregationMethod getMethod()
        {
            return method;
        }

        public boolean isDropOriginal()
        {
            return dropOriginal;
        }

        public boolean ruleApplied()
        {
            return aggregateName != null;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
                return true;
            if ( o == null || getClass() != o.getClass() )
                return false;
            Result result = (Result) o;
            return dropOriginal == result.dropOriginal &&
                Objects.equal( aggregateName, result.aggregateName ) &&
                method == result.method;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode( aggregateName, method, dropOriginal );
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper( this )
                          .add( "aggregateName", aggregateName )
                          .add( "method", method )
                          .add( "dropOriginal", dropOriginal )
                          .toString();
        }
    }

    public static MetricAggregationRule parseDefinition(String line, int id, boolean aggregationRuleCacheEnabled)
    {
        String[] parts = line.split( "=", 2 );
        String left_side = parts[0].trim();
        String right_side = parts[1].trim();

        String[] leftParts = left_side.split(" +");
        String outputPattern = leftParts[0];
        // "(60)" - strip parenthesis
        String freq = leftParts[1].replace( "(", "" ).replace(")", "").trim();
        int frequency = Integer.parseInt( freq );

        // optional flags - "drop", "c"
        boolean dropOriginal = false;
        boolean stopRule = true;
        if( leftParts.length > 2 )
        {

            for(int i = 2; i < leftParts.length; i++)
            {
                String flag = leftParts[i].trim().toLowerCase();
                if( "drop".equals( flag ) )
                {
                    dropOriginal = true;
                }
                else if( "c".equals( flag ) )
                {
                    stopRule = false;
                }
                else
                {
                    throw new RuntimeException( "Unsupported flag: [" + flag + "]" );
                }
            }
        }

        String[] rightParts = right_side.split( " " );
        String method = rightParts[0].trim().toUpperCase();
        String inputPattern = rightParts[1].trim();

        return new MetricAggregationRule(id, inputPattern, frequency, outputPattern,
            MetricAggregationMethod.valueOf( method ), dropOriginal, stopRule, aggregationRuleCacheEnabled);
    }


    public MetricAggregationRule(int id, String inputPattern, int frequency, String outputPattern,
                                 MetricAggregationMethod method, boolean dropOriginal, boolean stopRule,
                                 boolean aggregationRuleCacheEnabled)
    {
        this.id = id;
        this.inputPattern = Preconditions.checkNotNull( inputPattern );
        this.outputPattern = Preconditions.checkNotNull( outputPattern );
        this.stopRule = stopRule;
        this.aggregationRuleCacheEnabled = aggregationRuleCacheEnabled;

        Preconditions.checkArgument( frequency == 60,
            "Aggregation for frequency [%s] is not supported. For now only %s second frequency is supported",
            frequency, 60);

        this.method = Preconditions.checkNotNull( method );
        this.dropOriginal = dropOriginal;

        this.pattern = buildPattern();
        this.outputTemplate = buildOutputTemplate();
    }

    private Pattern buildPattern()
    {
        String[] inputPatternParts = inputPattern.split( "\\." );
        String[] regexParts = new String[inputPatternParts.length];

        for(int k = 0; k < inputPatternParts.length; k++)
        {
            String part = inputPatternParts[k];

            if(part.contains("<<") && part.indexOf( ">" ) > 0 )
            {
                int i = part.indexOf( "<<" );
                int j = part.indexOf( ">>" );
                if( j > i )
                {
                    String pre = part.substring( 0, i );
                    String post = part.substring( j + 2);
                    String fieldName = part.substring( i + 2, j );
                    fieldNames.add( fieldName );
                    regexParts[k] = String.format( "%s(?<%s>.+)%s", pre, fieldName, post );
                    continue;
                }
            }

            if(part.contains("<") && part.indexOf( ">" ) > 0 )
            {
                int i = part.indexOf( "<" );
                int j = part.indexOf( ">" );
                if( j > i )
                {
                    String pre = part.substring( 0, i );
                    String post = part.substring( j + 1);
                    String fieldName = part.substring( i + 1, j );
                    fieldNames.add( fieldName );
                    regexParts[k] = String.format( "%s(?<%s>[^.]+)%s", pre, fieldName, post );
                    continue;
                }
            }

            if( part.equals( "*" ))
            {
                regexParts[k] = "[^.]+";
            }
            else
            {
                regexParts[k] = part.replaceAll( "\\*", "[^.]*" );
            }
        }

        String pattern = "^" + String.join( "\\.", regexParts ) + "$";
        return Pattern.compile( pattern );
    }

    private String buildOutputTemplate()
    {
        return this.outputPattern.replaceAll("<", "%(").replaceAll( ">", ")" );
    }

    public Result apply(String name)
    {
        return new Result( aggregatedName( name ), method, dropOriginal );
    }

    private String aggregatedName(String name)
    {
        StringsCache.State state = aggregationRuleCacheEnabled ? StringsCache.getState(name) : null;
        if (state != null && state.getAggregationRuleMap().containsKey(id)
                && state.getAggregationRuleMap().get(id) == Boolean.FALSE) {
            return null;
        }

        Matcher m = pattern.matcher( name );
        boolean success = m.find();
        if (state != null) {
            state.getAggregationRuleMap().putIfAbsent(id, success);
        }
        if( success )
        {
            Map<String, String> fieldValues = new HashMap<>(  );
            for(String fieldName : fieldNames)
            {
                fieldValues.put( fieldName,  m.group( fieldName ));
            }

            //noinspection deprecation
            return StrSubstitutor.replace( outputTemplate, fieldValues, "%(", ")" );
        }
        else
        {
            return null;
        }
    }

    public MetricAggregationMethod getMethod()
    {
        return method;
    }

    public boolean isStopRule()
    {
        return stopRule;
    }
}
