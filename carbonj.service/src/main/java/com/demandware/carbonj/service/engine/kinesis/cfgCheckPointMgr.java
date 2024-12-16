/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.kinesis;

import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.accumulator.cfgAccumulator;
import com.demandware.carbonj.service.engine.CheckPointMgr;
import com.demandware.carbonj.service.engine.DynamoDbCheckPointMgr;
import com.demandware.carbonj.service.engine.FileCheckPointMgr;
import com.demandware.carbonj.service.engine.KinesisConfig;
import com.demandware.carbonj.service.engine.cfgKinesis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Configuration
@Import({ cfgAccumulator.class, cfgKinesis.class })
public class cfgCheckPointMgr {

    @Value( "${metrics.store.checkPoint.dir:work/carbonj-checkpoint}" ) private String checkPointDir;

    @Value( "${metrics.store.checkPoint.offset.default.mins:5}" ) private int defaultCheckPointOffset;

    @Value( "${metrics.store.checkPoint.provider:filesystem}" ) private String checkPointProvider;

    @Value( "${metrics.store.checkPoint.applicationName:cjajna}" ) private String checkPointApplicationName;

    @Value( "${metrics.store.checkPoint.table.provisioned.throughput:2}" ) private int
            checkPointTableProvisionedThroughput;

    @Value("${metrics.store.checkPoint.dynamodb.timeout:30}") private int checkPointDynamodbTimout;

    private static final Logger log = LoggerFactory.getLogger( cfgCheckPointMgr.class );

    @Bean
    CheckPointMgr<Date> checkPointMgr(ScheduledExecutorService s, KinesisConfig kinesisConfig,
                                      @Autowired( required = false ) @Qualifier( "accumulator" ) Accumulator accu )
            throws Exception
    {
        if (!kinesisConfig.isKinesisConsumerEnabled()) {
            log.warn("CheckPointMgr is disabled because kinesis consumer is disabled");
            return null;
        }
        if (accu == null)
        {
            log.warn("CheckPointMgr is disabled because accumulator is null");
            return null;
        }

        CheckPointMgr<Date> checkPointMgr;
        if ( checkPointProvider.equalsIgnoreCase( "dynamodb" ) )
        {
            log.info( "Creating Dynamo DB Checkpoint Mgr" );
            DynamoDbAsyncClient dynamoDbClient = DynamoDbAsyncClient.builder().build();
            checkPointMgr = new DynamoDbCheckPointMgr(dynamoDbClient, checkPointApplicationName,
                    defaultCheckPointOffset, checkPointTableProvisionedThroughput, checkPointDynamodbTimout);
        }
        else
        {
            log.info( "Creating File Checkpoint Mgr" );
            checkPointMgr = new FileCheckPointMgr( Paths.get( checkPointDir ), defaultCheckPointOffset );
        }

        s.scheduleWithFixedDelay( () -> {
            try
            {
                long slotTs = accu.getMaxClosedSlotTs() * 1000L;
                if ( slotTs > 0 )
                {
                    checkPointMgr.checkPoint( new Date( slotTs ) );
                }
            }
            catch ( Exception e )
            {
                log.error( "Error while checkpointing", e );
            }
        }, 120, 60, TimeUnit.SECONDS );
        return checkPointMgr;
    }
}
