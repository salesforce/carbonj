/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.admin.CarbonJClient;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.google.common.base.Throwables;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(SpringRunner.class)
@SpringBootTest
        (webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ContextConfiguration(
    initializers = ConfigDataApplicationContextInitializer.class)
@TestPropertySource(properties = { "spring.config.location=classpath:application-relay.yml" })

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class _SiteSpecificAccumulatorTest {

    final private static Logger log = LoggerFactory.getLogger(_SiteSpecificAccumulatorTest.class);

    @Value("${server.port}")
    int jettyHttpPort;

    @Value("${metrics.store.dataDir}")
    String dataDir;

    @Value("${server.dataport}")
    int jettyDataPort;

    @Value("${server.host}")
    String jettyHost;

    @Value("${metrics.store.enabled}")
    boolean metricsStoreEnabled;

    // The name of the property is like this so that cfgAccumulator initializes with the correct setting
    @Value("${metric.aggregation.rules}")
    String aggregationRuleFilePath;

    @Autowired
    private ApplicationContext context;

    private CarbonJClient cjClient = null;

    @PostConstruct
    private void initializeCjClient() {
        Assert.assertNotNull("jettyHost should not be null", jettyHost);
        Assert.assertNotEquals("jettyDataPort should not be 0", jettyDataPort);
        Assert.assertNotEquals("jettyHttpPort should not be 0", jettyHttpPort);
        cjClient = new CarbonJClient(jettyHost, jettyHttpPort, jettyDataPort);
    }

    @Before
    public void start() throws IOException {
        new File(dataDir).mkdirs();

        // start carbonj in relay mode only
        Path aggregationRulesFilePath = Paths.get(aggregationRuleFilePath);
        File aggregationRulesFile = aggregationRulesFilePath.toFile();

        // delete old file
        aggregationRulesFile.delete();
        setupAggregationRules(aggregationRulesFile);
        log.info("Aggregation Rule file Path " + aggregationRulesFilePath.toString());
    }

    private void setupAggregationRules(File file) throws IOException {
        String siteRule = "<pod>.ecom_site_ag.<realm>.<tenant>.<site>.<metric> (60) drop = custom1 <pod>.ecom.<realm>.<tenant>.*.*.site.<site>.<<metric>>";
        String originalRule = "<pod>.ecom_ag.<realm>.<tenant>.<metric> (60) = custom1 <pod>.ecom.<realm>.<tenant>.*.*.<<metric>>";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(siteRule);
            bw.newLine();
            bw.write(originalRule);
            bw.newLine();
        }
    }

    @Test
    public void testAlive() {
        // very basic test to ensure that server has started in "relay" config and accepts incoming messages
        DateTime dt0 = new DateTime();

        cjClient.send("pod.ecom.aagl.prd.blade1.prd.site.us.order.value", 1.0f, dt0);
        cjClient.send("pod.ecom.aagl.prd.blade2.prd.site.us.order.value", 2.0f, dt0);
        drain();

        // drain the accumulator points.
        Accumulator accumulator = context.getBean("accumulator", Accumulator.class);
        accumulator.rollUp(this::collectFlushedPoints, System.currentTimeMillis(), true);
    }

    private void collectFlushedPoints(DataPoints dataPoints) {
        Assert.assertEquals(1, dataPoints.size());
        DataPoint dataPoint = dataPoints.get(0);
        Assert.assertEquals("pod.ecom_site_ag.aagl.prd.us.order.value", dataPoint.name);
        Assert.assertEquals(3, dataPoint.val, 0.1);
    }

    private void drain() {
        // This is needed because if metrics store is not enabled, then
        // drain() throws this exception - org.springframework.beans.factory.NoSuchBeanDefinitionException: No qualifying bean of type 'com.demandware.carbonj.service.db.TimeSeriesStore' available
        if (metricsStoreEnabled) {
            this.context.getBean(TimeSeriesStore.class).drain();
        }

        try {
            Thread.sleep(10 * 500 + 100);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}