/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Destination that drops data points.
 */
public class NullDestination
    extends Destination
                implements LineProtocolDestination
{
    private static Logger log = LoggerFactory.getLogger( NullDestination.class );


    public NullDestination(MetricRegistry metricRegistry, String type, String name)
    {
        super(metricRegistry,"dest." + type + "." + name.replaceAll( "[:\\. /]", "_" ));
    }

    @Override
    public void accept( DataPoint t )
    {
        received.mark();

        if( log.isDebugEnabled() )
        {
            log.debug("Datapoint: ", t );
        }

        sent.mark();
    }

    @Override
    public void closeQuietly()
    {

    }
}
