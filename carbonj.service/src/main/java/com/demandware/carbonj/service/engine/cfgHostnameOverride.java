/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class cfgHostnameOverride
{
    @Bean( name = "localHostName" ) String localHostName()
                    throws UnknownHostException
    {
        String localhostName = InetAddress.getLocalHost().getHostName();
        String[] components = localhostName.split( "\\." );
        if ( components.length > 0 )
        {
            localhostName = components[0];
        }
        return localhostName;
    }
}
