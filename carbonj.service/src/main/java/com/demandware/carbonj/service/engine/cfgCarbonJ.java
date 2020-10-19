/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.accumulator.cfgAccumulator;
import com.demandware.carbonj.service.admin.CarbonjAdmin;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.cfgTimeSeriesStorage;
import com.demandware.carbonj.service.db.index.NameUtils;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.Quota;
import com.demandware.carbonj.service.engine.netty.NettyChannel;
import com.demandware.carbonj.service.engine.netty.NettyServer;
import com.demandware.carbonj.service.ns.NamespaceCounter;
import com.demandware.carbonj.service.ns.cfgNamespaces;
import com.demandware.carbonj.service.strings.StringsCache;
import com.demandware.carbonj.service.strings.cfgStrings;
import com.demandware.core.config.cfgMetric;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.demandware.carbonj.service.config.ConfigUtils.locateConfigFile;

@Configuration
@Import( { cfgTimeSeriesStorage.class, cfgHostnameOverride.class, cfgMetric.class, cfgCentralThreadPools.class,
                cfgStrings.class, cfgAccumulator.class, cfgNamespaces.class, cfgKinesis.class } )
public class cfgCarbonJ
{
    private static final Logger log = LoggerFactory.getLogger( cfgCarbonJ.class );

    @Autowired( required = false ) TimeSeriesStore db;

    @Autowired StringsCache stringsCache;

    @Value( "${server.port:56788}" ) private int jettyPort;

    @Value( "${jetty.logfilepath:log/request-yyyy_mm_dd.log}" ) private String jettyLogfilePath;

    @Value( "${line.protocol.udp.port:-1}" ) private int lineProtocolUdpPort;

    @Value( "${line.protocol.udp.host:0.0.0.0}" ) private String lineProtocolUdpHost;

    @Value( "${line.protocol.tcp.port:-1}" ) private int lineProtocolTcpPort;

    @Value( "${line.protocol.tcp.host:0.0.0.0}" ) private String lineProtocolTcpHost;

    @Value( "${jetty.maxFormContentSize:50000}" ) private int jettyMaxFormContentSize;

    @Value( "${netty.threads.io:0}" ) private int nettyIOThreads;

    @Value( "${netty.threads.work:0}" ) private int nettyWorkerThreads;

    @Value( "${relay.threads:1}" ) private int aggregatorThreads = 1;

    @Value( "${relay.queue:3000000}" ) private int aggregatorQueue = 3000000;

    @Value( "${aggregation.enabled:true}" ) private boolean aggregationEnabled;

    @Value( "${relay.queue.rejectPolicy:drop}" ) private String relayQueueRejectPolicy;

    @Value( "${relay.dest.queue:6000000}" ) private int destQueue;

    @Value( "${relay.dest.maxWaitTimeInSecs:10}" ) private int maxWaitTimeInSecs;

    @Value( "${relay.pickle.buff:2048576}" ) private int pickleBuff;

    @Value( "${relay.tcp.buff:1048576}" ) private int tcpBuff;

    @Value( "${relay.udp.buff:1048576}" ) private int udpBuff;

    @Value( "${relay.udp.msgbuff:4096}" ) private int udpMsgBuff;

    @Value( "${relay.dest.refreshIntervalInMillis:5000}" ) private int refreshIntervalInMillis;

    @Value( "${blacklist:config/blacklist.conf}" ) private String blacklistConfigFile = "config/blacklist.conf";

    @Value( "${blacklist:config/query-blacklist.conf}" ) private String queryBlacklistConfigFile =
                    "config/query-blacklist.conf";

    @Value( "${allowlist:config/allowlist.conf}") private String allowOnlyMetricsConfigFile = "config/allowlist.conf";

    @Value( "${relay.rules:config/relay-rules.conf}" ) private String relayRulesFile = "config/relay-rules.conf";

    @Value( "${audit.rules:config/audit-rules.conf}" ) private String auditRulesFile = "config/audit-rules.conf";

    @Value( "${consumerRules:config/consumer-rules.conf}" ) private String consumerRulesFile =
                    "config/consumer-rules.conf";

    @Value( "${destBatchSize:10000}" ) private int destBatchSize;

    @Value( "${inputQueue.batchSize:10000}" ) private int batchSize;

    @Value( "${inputQueue.emptyQueuePauseMillis:500}" ) private long emptyQueuePauseMillis;

    @Value( "${inputQueue.maxQueueReadsPerBatch:10}" ) private int maxQueueReadsPerBatch;

    @Value( "${inputQueue.refreshStatsInterval:1}" ) private int inputQueueRefreshStatsInterval;

    @Value( "${pointFilter.maxLen:300}" ) private int maxLen; // 300 characters

    @Value( "${pointFilter.maxAge:-1}" ) private int maxAge;

    @Value( "${pointFilter.maxFutureAge:-1}" ) private int maxFutureAge;

    @Value( "${pointFilter.enabled:true}" ) private boolean pointFilterEnabled;

    @Value( "${pointFilter.dupPointCacheMaxSize:12000000}" ) private int dupPointCacheMaxSize;

    @Value( "${pointFilter.dupPointCacheExpireInMin:60}" ) private int dupPointCacheExpireInMin;

    @Value( "${pointFilter.errLogQuota.max:1000}" ) private int errLogQuotaMax = 1000;

    @Value( "${pointFilter.errLogQuota.resetAfter:3600}" ) private int errLogQuotaResetAfter = 3600;

    // set to 0 to disable - don't need this overhead in relay.
    @Value( "${namespaces.runInactiveNamespaceCheckEverySeconds:300}" ) private int
                    runInactiveNamespaceCheckEverySeconds = 300;

    @Value( "${metrics.store.checkPoint.dir:work/carbonj-checkpoint}" ) private String checkPointDir;

    @Value( "${metrics.store.checkPoint.offset.default.mins:5}" ) private int defaultCheckPointOffset;

    @Value( "${metrics.store.checkPoint.provider:filesystem}" ) private String checkPointProvider;

    @Value( "${metrics.store.checkPoint.applicationName:cjajna}" ) private String checkPointApplicationName;

    @Value( "${metrics.store.checkPoint.table.provisioned.throughput:2}" ) private int
                    checkPointTableProvisionedThroughput;

    @Value( "${dest.config.dir:config}" ) private String destConfigDir;

    @Value( "${app.servicedir:}" ) private String serviceDir;

    @Autowired MetricRegistry metricRegistry;

    @Bean( name = "dataPointSinkRelay" ) Relay relay( ScheduledExecutorService s )
    {
        File rulesFile = locateConfigFile( serviceDir, relayRulesFile );
        Relay r = new Relay( metricRegistry, "relay", rulesFile, destQueue, destBatchSize, refreshIntervalInMillis,
                        destConfigDir, maxWaitTimeInSecs );
        s.scheduleWithFixedDelay( r::reload, 15, 30, TimeUnit.SECONDS );
        return r;
    }

    @Bean( name = "auditLogRelay" ) Relay auditLog( ScheduledExecutorService s )
    {
        File rulesFile = locateConfigFile( serviceDir, auditRulesFile );
        Relay r = new Relay( metricRegistry, "audit", rulesFile, destQueue, destBatchSize, refreshIntervalInMillis,
                        destConfigDir, maxWaitTimeInSecs );
        s.scheduleWithFixedDelay( r::reload, 15, 30, TimeUnit.SECONDS );
        return r;
    }

    @Bean NameUtils nameUtils()
    {
        return new NameUtils( new Quota( errLogQuotaMax, errLogQuotaResetAfter ) );
    }

    @Bean PointFilter pointFilter( NameUtils nameUtils )
    {
        return new PointFilter( metricRegistry, "pointFilter", maxLen, maxAge, maxFutureAge, nameUtils,
                        dupPointCacheMaxSize, dupPointCacheExpireInMin,
                        new Quota( errLogQuotaMax, errLogQuotaResetAfter ) );
    }

    @Bean( name = "pointBlacklist" ) MetricList pointBlacklist( ScheduledExecutorService s )
    {
        MetricList bs = new MetricList( metricRegistry, "blacklist",
                        locateConfigFile( serviceDir, blacklistConfigFile ) );
        s.scheduleWithFixedDelay( bs::reload, 10, 30, TimeUnit.SECONDS );
        return bs;
    }

    @Bean( name = "queryBlacklist" ) MetricList queryBlacklist( ScheduledExecutorService s )
    {
        MetricList bs = new MetricList( metricRegistry, "queryBlacklist",
                        locateConfigFile( serviceDir, queryBlacklistConfigFile ) );
        s.scheduleWithFixedDelay( bs::reload, 10, 30, TimeUnit.SECONDS );
        return bs;
    }

    @Bean( name = "pointAllowOnlyList" ) MetricList pointAllowOnlyList( ScheduledExecutorService s )
    {
        MetricList metricList = new MetricList( metricRegistry, "allowOnly", locateConfigFile( serviceDir, allowOnlyMetricsConfigFile ) );
        s.scheduleWithFixedDelay( metricList::reload, 10, 30, TimeUnit.SECONDS );
        return metricList;
    }


    @Bean @DependsOn( "stringsCache" ) PointProcessor pointProcessor(
                    @Qualifier( "datapoint_sink" ) Consumer<DataPoints> sink, ScheduledExecutorService s,
                    @Qualifier( "pointBlacklist" ) MetricList blacklist,
                    @Qualifier( "pointAllowOnlyList" ) MetricList allowOnly,
                    @Qualifier( "auditLogRelay" ) Relay auditLog,
                    PointFilter pointFilter, @Qualifier( "accumulator" ) Accumulator accumulator,
                    NamespaceCounter nsCounter )
    {

        if ( !pointFilterEnabled )
        {
            pointFilter = null;
        }

        if ( !aggregationEnabled )
        {
            log.warn( "Aggregate generation is disabled. Use 'aggregation.enabled' to control." );
        }

        PointProcessorTaskBuilder taskBuilder =
                        new PointProcessorTaskBuilder( metricRegistry, sink, blacklist, allowOnly, auditLog,
                                aggregationEnabled, pointFilter, accumulator, nsCounter );

        PointProcessor pointProcessor =
                        new PointProcessorImpl( metricRegistry, "pointProcessor", aggregatorThreads, taskBuilder );

        if ( aggregationEnabled )
        {
            s.scheduleWithFixedDelay( () -> {
                // flush aggregations takes few seconds. it results in gradual offset of start time.
                // so in reality the task runs not every 60s but 62-63 - creating accumulated error
                // the workaround it to return immediately by submitting flush asynchronously.
                s.submit( () -> pointProcessor.flushAggregations( false ) );
            }, 15, 15, TimeUnit.SECONDS );
        }

        return pointProcessor;
    }

    @Bean( name = "recoveryPointProcessor" ) @DependsOn( "stringsCache" ) PointProcessor recoveryPointProcessor(
                    @Qualifier( "datapoint_sink" ) Consumer<DataPoints> sink, ScheduledExecutorService s,
                    @Qualifier( "pointBlacklist" ) MetricList blacklist,
                    @Qualifier( "pointAllowOnlyList" ) MetricList allowOnly,
                    @Qualifier( "auditLogRelay" ) Relay auditLog,
                    @Qualifier( "recoveryAccumulator" ) Accumulator accumulator, NamespaceCounter nsCounter,
                    KinesisConfig kinesisConfig, NameUtils nameUtils )
    {

        if ( !kinesisConfig.isRecoveryEnabled() )
        {
            return null;
        }

        PointFilter pointFilter;
        if ( !pointFilterEnabled )
        {
            pointFilter = null;
        }
        else
        {
            // create a point filter with duplicate checking disabled.
            pointFilter = new PointFilter( metricRegistry, "recoveryPointFilter", maxLen, maxAge, maxFutureAge,
                            nameUtils, -1, dupPointCacheExpireInMin,
                            new Quota( errLogQuotaMax, errLogQuotaResetAfter ) );
        }

        if ( !aggregationEnabled )
        {
            log.warn( "Aggregate generation is disabled. Use 'aggregation.enabled' to control." );
        }

        PointProcessorTaskBuilder taskBuilder =
                        new PointProcessorTaskBuilder( metricRegistry, sink, blacklist, allowOnly, auditLog,
                                aggregationEnabled, pointFilter, accumulator, nsCounter );

        PointProcessor pointProcessor = new PointProcessorImpl( metricRegistry, "pointProcessorRecovery",
                        kinesisConfig.recoveryThreads(), taskBuilder );

        if ( aggregationEnabled )
        {
            s.scheduleWithFixedDelay( () -> {
                // flush aggregations takes few seconds. it results in gradual offset of start time.
                // so in reality the task runs not every 60s but 62-63 - creating accumulated error
                // the workaround it to return immediately by submitting flush asynchronously.
                s.submit( () -> pointProcessor.flushAggregations( false ) );
            }, 15, 15, TimeUnit.SECONDS );
        }

        return pointProcessor;
    }

    @Bean InputQueue inputQueue( PointProcessor pointProcessor )
    {
        return new InputQueue( metricRegistry, "input-queue-consumer", pointProcessor, aggregatorQueue,
                        relayQueueRejectPolicy, batchSize, emptyQueuePauseMillis );
    }

    @Bean( name = "datapoint_sink" ) Consumer<DataPoints> dataPointSink( @Qualifier( "dataPointSinkRelay" ) Relay r )
    {
        if ( db != null )
        {
            return db.andThen( r );
        }
        else
        {
            return r;
        }
    }

    @Autowired( required = false ) CheckPointMgr<Date> checkPointMgr;

    @Bean Consumers consumer( PointProcessor pointProcessor,
                              @Qualifier( "recoveryPointProcessor" ) PointProcessor recoveryPointProcessor,
                              ScheduledExecutorService s, KinesisConfig kinesisConfig )
    {
        if ( kinesisConfig.isKinesisConsumerEnabled() )
        {
            File rulesFile = locateConfigFile( serviceDir, consumerRulesFile );
            Consumers consumer = new Consumers( metricRegistry, pointProcessor, recoveryPointProcessor, rulesFile,
                            kinesisConfig, checkPointMgr );
            s.scheduleWithFixedDelay( consumer::reload, 15, 30, TimeUnit.SECONDS );
            return consumer;
        }
        else
            return null;
    }

    @Bean Void stats( ScheduledExecutorService s, @Qualifier( "dataPointSinkRelay" ) Relay r, InputQueue a,
                      @Qualifier( "pointBlacklist" ) MetricList pbl, @Qualifier( "queryBlacklist" ) MetricList qbl,
                      @Qualifier( "pointAllowOnlyList" ) MetricList pal,
                      NettyServer nettyServer, @Qualifier( "auditLogRelay" ) Relay auditLog, StringsCache strCache,
                      @Qualifier( "accumulator" ) Accumulator accu )
    {

        s.scheduleWithFixedDelay( () -> {
            a.dumpStats();
            pbl.dumpStats();
            qbl.dumpStats();
            pal.dumpStats();
            r.dumpStats();
            auditLog.dumpStats();
            if ( db != null )
            {
                db.dumpStats();
            }
            nettyServer.dumpStats();
            strCache.dumpStats();
            if ( accu != null )
            {
                accu.dumpStats();
            }
        }, 60, 60, TimeUnit.SECONDS );
        return null;
    }

    @Bean Void checkExpiredNamespaces( ScheduledExecutorService s, NamespaceCounter ns )
    {
        if ( runInactiveNamespaceCheckEverySeconds > 0 )
        {
            log.info( String.format( "scheduling removal of expired namespace counters to run every %s sec",
                            runInactiveNamespaceCheckEverySeconds ) );
            s.scheduleWithFixedDelay( ns::removeInactive, 120, runInactiveNamespaceCheckEverySeconds,
                            TimeUnit.SECONDS );
        }
        else
        {
            log.info( "removal of expired namespace counters is not configured." );
        }
        return null;
    }

    @Bean Void refreshStats( ScheduledExecutorService s, InputQueue a, @Qualifier( "accumulator" ) Accumulator accu )
    {
        s.scheduleWithFixedDelay( () -> {
            a.refreshStats();
            if ( accu != null )
            {
                accu.refreshStats();
            }
            if ( db != null )
            {
                db.refreshStats();
            }
        }, 60, inputQueueRefreshStatsInterval, TimeUnit.SECONDS );
        return null;
    }

    @Bean NettyServer netty()
    {
        return new NettyServer( metricRegistry, nettyIOThreads, nettyWorkerThreads );
    }

    @Bean NettyChannel lineProtocolChannel( NettyServer netty, InputQueue r )
    {
        lineProtocolTcpPort = ( lineProtocolTcpPort == -1 ) ? jettyPort + 2 : lineProtocolTcpPort;
        return netty.bind( lineProtocolTcpHost, lineProtocolTcpPort, new ChannelInitializer<SocketChannel>()
        {
            @Override public void initChannel( SocketChannel ch )
            {
                if ( log.isDebugEnabled() )
                {
                    log.debug( "accepted TCP line protocol from " + ch );
                }
                ch.pipeline().addLast( new DelimiterBasedFrameDecoder( tcpBuff, Delimiters.lineDelimiter() ),
                                new LineProtocolHandler( metricRegistry, r ) );
            }
        } );
    }

    @Bean NettyChannel udpLineProtocolChannel( NettyServer netty, InputQueue r )
    {
        lineProtocolUdpPort = ( lineProtocolUdpPort == -1 ) ? jettyPort + 2 : lineProtocolUdpPort;
        NettyChannel channel = netty.udpBind( lineProtocolUdpHost, lineProtocolUdpPort, udpBuff, udpMsgBuff,
                        new SimpleChannelInboundHandler<DatagramPacket>()
                        {
                            LineProtocolHandler lp = new LineProtocolHandler( metricRegistry, r );

                            @Override protected void channelRead0( ChannelHandlerContext ctx, DatagramPacket msg )
                            {
                                // temporary metric used to correlate number of messages sent with number of messages received.
                                NettyServer.udpMsgsReceived.mark();
                                if ( log.isDebugEnabled() )
                                {
                                    log.debug( "accepted UDP line protocol from " + ctx.channel() );
                                }
                                lp.process( msg.content() );
                            }
                        } );

        channel.checkUdpChannelConfig( udpBuff, udpMsgBuff );

        return channel;
    }

    @Bean NettyChannel pickleProtocolChannel( NettyServer netty, InputQueue r )
    {
        return netty.bind( "0.0.0.0", jettyPort + 3, new ChannelInitializer<SocketChannel>()
        {
            @Override public void initChannel( SocketChannel ch )
            {
                if ( log.isDebugEnabled() )
                {
                    log.debug( "accepted pickle protocol from " + ch );
                }

                ch.pipeline().addLast( new LengthFieldBasedFrameDecoder( pickleBuff, 0, 4 ) )
                                .addLast( new PickleProtocolHandler( metricRegistry, r ) );
            }
        } );
    }

    @Bean CarbonjAdmin cjAdmin( InputQueue agg, NameUtils nu )
    {
        return new CarbonjAdmin( agg, nu, Optional.ofNullable( db ) );
    }

    @Bean CheckPointMgr<Date> checkPointMgr( ScheduledExecutorService s, KinesisConfig kinesisConfig,
                                             @Autowired( required = false ) @Qualifier( "accumulator" ) Accumulator accu )
                    throws Exception
    {
        if ( !kinesisConfig.isKinesisConsumerEnabled() || accu == null )
        {
            log.debug( "CheckPointMgr is disabled because kinesis consumer is disabled or accumulator is null: Kinesis"
                            + " consumer: {}, Accumulator: {}", kinesisConfig.isKinesisConsumerEnabled(), accu );
            return null;
        }

        CheckPointMgr<Date> checkPointMgr;
        if ( checkPointProvider.equalsIgnoreCase( "dynamodb" ) )
        {
            log.info( "Creating Dynamo DB Checkpoint Mgr" );
            AmazonDynamoDB dynamoDbClient = AmazonDynamoDBClientBuilder.standard().build();
            checkPointMgr = new DynamoDbCheckPointMgr( dynamoDbClient, checkPointApplicationName,
                            defaultCheckPointOffset, checkPointTableProvisionedThroughput );
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

    /*
    Graphite re-configuration summary:

    1. Configure remote data server

    In settings.py around line 50 specify carbonj as destination:

    CLUSTER_SERVERS = ["10.0.2.2:56787"]

    2. Disable local store

    In settings.py around line 150 replace "DATA_DIRS = [WHISPER_DIR]" with "DATA_DIRS = []"

    3. Modify Store class implementation to not throw exception when local store is not available

    In storage.py around line 33 comment out two lines that validate directory configuration

    class Store:
    .....
    #    if not (directories or remote_hosts):
    #      raise valueError("directories and remote_hosts cannot both be empty")
    */

    @Bean public ServletRegistrationBean graphiteMetricSearchServlet()
    {
        ServletRegistrationBean servletRegistration =
                        new ServletRegistrationBean( new GraphiteMetricSearchServlet(), "/metrics/*" );
        servletRegistration.setLoadOnStartup( 1 );
        return servletRegistration;
    }

    @Bean public ServletRegistrationBean graphiteSeriesDataServlet()
    {
        ServletRegistrationBean servletRegistration =
                        new ServletRegistrationBean( new GraphiteSeriesDataServlet(), "/render/*" );
        servletRegistration.setLoadOnStartup( 1 );
        return servletRegistration;
    }

    @Bean public ServletRegistrationBean graphiteSeriesDataTestServlet()
    {
        ServletRegistrationBean servletRegistration =
                        new ServletRegistrationBean( new GraphiteSeriesDataTestServlet(), "/render-test/*" );
        servletRegistration.setLoadOnStartup( 1 );
        return servletRegistration;
    }

    @Autowired private Environment environment;

    @PostConstruct public void getActiveProfiles()
    {
        for ( String profileName : environment.getActiveProfiles() )
        {
            log.warn( "Currently active profile: " + profileName );
        }
    }

    @Component
    public class JettyContainer
                    implements WebServerFactoryCustomizer<JettyServletWebServerFactory>
    {
        @Override public void customize( JettyServletWebServerFactory factory )
        {
            factory.addServerCustomizers( server -> {
                server.setAttribute( "org.eclipse.jetty.server.Request.maxFormContentSize", jettyMaxFormContentSize );

                // TODO change to use logback ch.qos.logback.access.jetty.RequestLogImpl?
                NCSARequestLog ncsaLog = new NCSARequestLog( jettyLogfilePath );
                ncsaLog.setExtended( true );
                ncsaLog.setAppend( true );
                ncsaLog.setLogTimeZone( "GMT" );
                ncsaLog.setRetainDays( 90 );

                RequestLogHandler requestLogHandler = new RequestLogHandler();
                requestLogHandler.setRequestLog( ncsaLog );
                requestLogHandler.setHandler( server.getHandler() );
                server.setHandler( requestLogHandler );
            } );
        }
    }

    @Bean public DatabaseMetrics databaseMetrics( MetricRegistry metricRegistry )
    {
        return new DatabaseMetrics( metricRegistry );
    }
}
