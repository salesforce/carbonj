/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.admin.CarbonJClient;
import com.demandware.carbonj.service.admin.CarbonjAdmin;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.google.common.base.Throwables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.PostConstruct;

@RunWith( SpringRunner.class )
@SpringBootTest
        (webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("testLongId")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@FixMethodOrder( MethodSorters.NAME_ASCENDING)
public abstract class CarbonJSvcLongIdTest extends AbstractCarbonJBaseTest
{
    @Value( "${server.port}" ) int jettyHttpPort;

    @Value( "${server.dataport}" ) int jettyDataPort;

    @Value( "${server.host}" ) String jettyHost;

    @Value( "${metrics.store.enabled}" ) boolean metricsStoreEnabled;

    @Autowired protected PointFilter pointFilter;

    @Autowired protected CarbonjAdmin carbonjAdmin;

    @Autowired protected MetricIndex metricIndex;

    protected CarbonJClient cjClient = null;

    private static final Logger log = LoggerFactory.getLogger( _CarbonJSvcTest.class );

    protected static final String DB_60S = "60s24h";

    protected static final String DB_5M = "5m7d";

    protected static final String DB_30M = "30m2y";

    @Autowired
    protected TimeSeriesStore timeSeriesStore;

    @PostConstruct
    private void initializeCjClient() {
        Assert.assertNotNull( "jettyHost should not be null", jettyHost );
        Assert.assertNotEquals( "jettyDataPort should not be 0", jettyDataPort );
        Assert.assertNotEquals( "jettyHttpPort should not be 0", jettyHttpPort );
        cjClient = new CarbonJClient( jettyHost, jettyHttpPort, jettyDataPort );
    }

    @Before
    public void before()
    {
        dropData();
    }

    protected void dropData()
    {
        timeSeriesStore.deleteAll();
        pointFilter.reset();
    }

    @After
    public void drain()
    {
        log.info("Starting to drain TimeSeriesStore...");

        // This is needed because if metrics store is not enabled, then
        // drain() throws this exception - org.springframework.beans.factory.NoSuchBeanDefinitionException: No qualifying bean of type 'com.demandware.carbonj.service.db.TimeSeriesStore' available
        if ( metricsStoreEnabled )
        {
            timeSeriesStore.drain();
        }

        try
        {
            Thread.sleep( 10 * 500 + 100 );
        }
        catch ( InterruptedException e )
        {
            throw Throwables.propagate( e );
        }
    }
}
