/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.accumulator;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.recovery.RecoveryAccumulator;
import com.demandware.carbonj.service.accumulator.recovery.RecoveryLatePointLogger;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.engine.KinesisConfig;
import com.demandware.carbonj.service.engine.cfgKinesis;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.demandware.carbonj.service.ns.cfgNamespaces;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.demandware.carbonj.service.config.ConfigUtils.locateConfigFile;

@Configuration
@Import( { cfgNamespaces.class, cfgKinesis.class} )
public class cfgAccumulator
{
    @Autowired( required = false )
    TimeSeriesStore db;

    @Value( "${metricAggregation.flushBatchSize:10000}" )
    private int batchSize;

    @Value( "${metric.aggregation.rules:config/aggregation-rules.conf}" )
    private String metricAggregationRulesConfigFile = "config/aggregation-rules.conf";

    @Value( "${metric.aggregation.slotMaxLifeSec:120}")
    private int slotMaxLifeSec;

    @Value( "${metric.aggregation.recoveryMaxSlots:3}")
    private int recoveryMaxSlots;

    @Value( "${aggregation.enabled:true}" )
    private boolean aggregationEnabled;

    @Value( "${aggregation.rule.cache.enabled:false}" )
    private boolean aggregationRuleCacheEnabled;

    // TODO duplicated in different cfg beans
    @Value( "${app.servicedir:}" )
    private String serviceDir;

    @Autowired
    MetricRegistry metricRegistry;

    @Bean(name = "accumulator")
    Accumulator accumulator(ScheduledExecutorService s, SlotStrategy slotStrategy, NamespaceCounter ns)
    {
        if (!aggregationEnabled) {
            return null;
        }

        MetricAggregationPolicyProvider policyProvider = getMetricAggregationPolicyProvider(s);
        return new AccumulatorImpl( metricRegistry, policyProvider, batchSize, slotMaxLifeSec, slotStrategy, ns);
    }

    private MetricAggregationPolicyProvider getMetricAggregationPolicyProvider(ScheduledExecutorService s) {
        File rulesFile = locateConfigFile( serviceDir, metricAggregationRulesConfigFile );
        MetricAggregationRulesLoader rulesLoader = new MetricAggregationRulesLoader( rulesFile, aggregationRuleCacheEnabled );
        s.scheduleWithFixedDelay(rulesLoader::reload, 60, 45, TimeUnit.SECONDS );

        MetricAggregationPolicySource policySource = new MetricAggregationPolicySource( rulesLoader );

        MetricAggregationPolicyProvider policyProvider;

        if( db != null )
        {
            // takes advantage of metrics cache
            policyProvider = new MetricAggregationPolicyProviderWithDb( db,  policySource);
        }
        else
        {
            policyProvider = new MetricAggregationPolicyProviderWithoutDb( policySource );
        }
        return policyProvider;
    }

    @Bean(name = "recoveryAccumulator")
    Accumulator recoveryAccumulator(MetricRegistry metricRegistry, ScheduledExecutorService s, SlotStrategy slotStrategy,
                                    NamespaceCounter ns, KinesisConfig kinesisConfig)
    {
        if (!aggregationEnabled  || !kinesisConfig.isRecoveryEnabled()) {
            return null;
        }

        MetricAggregationPolicyProvider policyProvider = getMetricAggregationPolicyProvider(s);

        return new RecoveryAccumulator( metricRegistry, "recovery.accumulator", policyProvider, batchSize, recoveryMaxSlots,
                slotStrategy, new RecoveryLatePointLogger(metricRegistry), ns);
    }

    @Bean
    SlotStrategy slotStrategy() {
        return new DefaultSlotStrategy();
    }
}
