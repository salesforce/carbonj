/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.core.metric;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtil
{
    private static String falconPrefix = "cc-umon-client.falcon-fi";
    private static String umDevPrefix = "um.dev.carbonj.";

    @Test
    public void testGetGraphiteMetricPrefix() throws UnknownHostException
    {
        String fqdn = InetAddress.getLocalHost().getHostName();
        String hostname =  fqdn.substring(0, fqdn.indexOf( "." ) == -1 ? fqdn.length() : fqdn.indexOf( "." ) );

        // Verify provided prefix with hostname
        assertEquals(falconPrefix + "." + hostname, Util.getGraphiteMetricPrefix(falconPrefix, true, -1, "00", "v1"));

        // Verify provided prefix without hostname
        assertEquals(falconPrefix, Util.getGraphiteMetricPrefix(falconPrefix, false, -1, "00", "v1"));

        // Verify prefix if podId is provided
        assertEquals("pod100.00.carbonj." + hostname + ".v1", Util.getGraphiteMetricPrefix(null, false, 100, "00", "v1"));

        // Verify prefix if podId and prefix aren't provided
        assertEquals(umDevPrefix + hostname, Util.getGraphiteMetricPrefix(null, false, -1, "00", "v1"));
    }
}