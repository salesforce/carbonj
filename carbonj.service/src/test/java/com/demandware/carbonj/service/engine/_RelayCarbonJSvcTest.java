/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.admin.CarbonJClient;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith( SpringRunner.class )
@DirtiesContext( classMode = DirtiesContext.ClassMode.AFTER_CLASS )
@TestPropertySource( "classpath:application-relay.yml" )
@SpringBootTest( classes = CarbonJServiceMain.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT )
public class _RelayCarbonJSvcTest extends AbstractCarbonJBaseTest
{
    private static final Logger log = LoggerFactory.getLogger( _RelayCarbonJSvcTest.class );

    @Value( "${server.port}" ) int jettyHttpPort;

    @Value( "${server.dataport}" ) int jettyDataPort;

    @Value( "${server.host}" ) String jettyHost;

    @Value( "${metrics.store.enabled}" ) boolean metricsStoreEnabled;

    /**
     * Very basic test to ensure that server has started in "relay" config and accepts incoming messages
     */
    @Test public void testAlive()
    {

        DateTime dt0 = new DateTime();
        DateTime dt1 = dt0.plusMinutes( 1 );
        log.info( "Jetty host used for the test " + jettyHost );
        log.info( "Jetty http port used for the test  " + jettyHttpPort );
        log.info( "Jetty data port used for the test  " + jettyDataPort );

        CarbonJClient cjClient = new CarbonJClient( jettyHost, jettyHttpPort, jettyDataPort );
        cjClient.send( "testData", 1.0f, dt0 );
        cjClient.send( "testData", 2.0f, dt1 );
    }

}
