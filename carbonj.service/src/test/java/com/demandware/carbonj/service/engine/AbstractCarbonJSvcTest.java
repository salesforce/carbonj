/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.admin.CarbonJClient;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import javax.annotation.PostConstruct;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
        (webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class AbstractCarbonJSvcTest extends AbstractCarbonJBaseTest
{
    @Value( "${server.port}" ) int jettyHttpPort;

    @Value( "${server.dataport}" ) int jettyDataPort;

    @Value( "${server.host}" ) String jettyHost;

    @Value( "${metrics.store.enabled}" ) boolean metricsStoreEnabled;

    protected CarbonJClient cjClient = null;

    private static final Logger log = LoggerFactory.getLogger( _CarbonJSvcTest.class );

    @Autowired
    protected TimeSeriesStore timeSeriesStore;

    @PostConstruct
    private void initializeCjClient() {
        assertNotNull( "jettyHost should not be null", jettyHost );
        assertNotEquals( "jettyDataPort should not be 0", jettyDataPort );
        assertNotEquals( "jettyHttpPort should not be 0", jettyHttpPort );
        cjClient = new CarbonJClient( jettyHost, jettyHttpPort, jettyDataPort );
    }

    @AfterEach
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
            throw new RuntimeException(e);
        }
    }
}
