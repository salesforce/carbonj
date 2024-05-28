/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class QueryUtils
{

    private static final Logger log = LoggerFactory.getLogger( QueryUtils.class );

    public static String patternToRegEx(String p)
    {
        String r = p.replaceAll("\\*", ".*");
        r = r.replaceAll("\\{", "(");
        r = r.replaceAll("\\}", ")");
        r = r.replaceAll(",", "|");

        return "^" + r + "$";
    }

    public static List<String> filter(List<String> entries, QueryPart pattern, LoadingCache<String, Pattern> queryPatternCache) {
        List<String> matched = new ArrayList<>();
        for (String entry : entries) {
            try {
                if (match(entry, pattern, queryPatternCache)) {
                    matched.add(entry);
                }
            } catch (ExecutionException e) {
                log.error("Failed to check query pattern {} for query {} - {}", pattern.getQuery(), entry, e.getMessage());
            }
        }
        return matched;
    }

    /*
    http://graphite.readthedocs.org/en/latest/render_api.html

        The asterisk (*) matches zero or more characters. It is non-greedy, so you can have more than one within a single path element.
        Example: servers.ix*ehssvc*v.cpu.total.* will return all total CPU metrics for all servers matching the given name pattern.

        [...] - character list or range. single char pos in the path string
        {...} - comma separated value list

        All wildcards apply only within a single path element.
        In other words, they do not include or cross dots (.). Therefore, servers.* will not match servers.ix02ehssvc04v.cpu.total.user, while servers.*.*.*.* will.

        TODO: '?' - what about this one?
     */
    public static boolean isPattern(String s)
    {
        return s.indexOf( '*' ) > -1 || s.indexOf( '[' ) > -1 || s.indexOf( '{' ) > -1;
    }

    public static QueryPart[] splitQuery(String query)
    {
        String[] parts = query.split( "\\." );
        QueryPart[] queryParts = new QueryPart[parts.length];
        for(int i = 0; i < parts.length; i++)
        {
            if( isPattern(parts[i]) )
            {
                queryParts[i] = new QueryPart(patternToRegEx(parts[i]), true);
            } else {
                queryParts[i] = new QueryPart(parts[i], false);
            }
        }
        return queryParts;
    }


    //    def match_entries(entries, pattern):
    //                # First we check for pattern variants (ie. {foo,bar}baz = foobaz or barbaz)
    //    v1, v2 = pattern.find('{'), pattern.find('}')
    //
    //                if v1 > -1 and v2 > v1:
    //    variations = pattern[v1+1:v2].split(',')
    //    variants = [ pattern[:v1] + v + pattern[v2+1:] for v in variations ]
    //    matching = []
    //
    //                for variant in variants:
    //                matching.extend( fnmatch.filter(entries, variant) )
    //
    //                return list( _deduplicate(matching) ) #remove dupes without changing order
    //
    //    else:
    //    matching = fnmatch.filter(entries, pattern)
    //                matching.sort()
    //                return matching


    public static boolean match(String namePart, QueryPart pattern, LoadingCache<String, Pattern> queryPatternCache) throws ExecutionException {
        if (pattern.isRegEx()) {
            if (queryPatternCache != null) {
                Matcher matcher = queryPatternCache.get(pattern.getQuery()).matcher(namePart);
                return matcher.matches();
            } else {
                return namePart.matches(pattern.getQuery());
            }
        } else {
            return namePart.equals(pattern.getQuery());
        }
    }
}
