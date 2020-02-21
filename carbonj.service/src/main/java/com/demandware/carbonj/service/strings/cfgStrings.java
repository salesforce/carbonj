/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.strings;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class cfgStrings
{
    @Value( "${string.cache.minSize:5000000}")
    private int commonStringsMinCacheSize = 5000000;

    @Value( "${string.cache.maxSize:10000000}")
    private int commonStringsMaxCacheSize = 10000000;

    @Value( "${string.cache.expireAfterLastAccessInMinutes:180}")
    private int expireAfterLastAccessInMinutes = 180;


    @Value( "${string.cache.concurrencyLevel:8}")
    private int concurrencyLevel = 8;

    @Bean
    StringsCache stringsCache(MetricRegistry metricRegistry)
    {
        return new StringsCache( metricRegistry, commonStringsMinCacheSize, commonStringsMaxCacheSize, expireAfterLastAccessInMinutes, concurrencyLevel );
    }
}
