/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.ns;

import com.codahale.metrics.MetricRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration public class cfgNamespaces
{
    @Value( "${namespaces.removeInactiveAfterSeconds:7200}")
    private int removeInactiveAfterSeconds = 7200; // 2hours

    @Autowired
    MetricRegistry metricRegistry;

    @Bean NamespaceCounter namespaceCounter()
    {
        return new NamespaceCounter(metricRegistry, removeInactiveAfterSeconds);
    }

}
