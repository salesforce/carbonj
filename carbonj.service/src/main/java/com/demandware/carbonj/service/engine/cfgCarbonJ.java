/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.kinesis.cfgCheckPointMgr;
import com.demandware.carbonj.service.events.cfgEventBus;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.accumulator.cfgAccumulator;
import com.demandware.carbonj.service.admin.CarbonjAdmin;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.cfgTimeSeriesStorage;
import com.demandware.carbonj.service.db.index.NameUtils;
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
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.demandware.carbonj.service.config.ConfigUtils.locateConfigFile;

@Configuration
@Import( { cfgMetric.class, cfgTimeSeriesStorage.class, cfgHostnameOverride.class, cfgCentralThreadPools.class,
        cfgStrings.class, cfgAccumulator.class, cfgNamespaces.class, cfgKinesis.class, cfgEventBus.class,
        cfgCheckPointMgr.class, cfgAws.class } )
public class cfgCarbonJ
{
    private static final Logger log = LoggerFactory.getLogger( cfgCarbonJ.class );

    @Autowired( required = false ) TimeSeriesStore db;

    @Autowired StringsCache stringsCache;

    @Autowired MetricRegistry metricRegistry;

    @Autowired NameUtils nameUtils;

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

    @Value( "${metriclistConfigSrc:file}" ) private String metricListConfigSrc;

    @Value( "${blacklist:config/query-blacklist.conf}" ) private String queryBlacklistConfigFile =
                    "config/query-blacklist.conf";

    @Value( "${allowlist:config/allowlist.conf}") private String allowOnlyMetricsConfigFile = "config/allowlist.conf";

    @Value( "${relay.rules:config/relay-rules.conf}" ) private String relayRulesFile = "config/relay-rules.conf";

    // Relay rules can be pulled from file or server
    @Value( "${relay.configSrc:file}" ) private String relayRulesSrc;

    @Value("${relay.cache.enabled:true}") private boolean relayCacheEnabled;

    @Value( "${audit.rules:config/audit-rules.conf}" ) private String auditRulesFile = "config/audit-rules.conf";

    @Value( "${audit.rules.configSrc:file}" ) private String auditRulesSrc;

    @Value( "${consumerRules:config/consumer-rules.conf}" ) private String consumerRulesFile =
                    "config/consumer-rules.conf";

    @Value( "${destBatchSize:10000}" ) private int destBatchSize;

    @Value( "${inputQueue.batchSize:10000}" ) private int batchSize;

    @Value( "${inputQueue.emptyQueuePauseMillis:500}" ) private long emptyQueuePauseMillis;

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


    @Value( "${dest.config.dir:config}" ) private String destConfigDir;

    @Value( "${app.servicedir:}" ) private String serviceDir;

    @Value( "${metrics.store.dataDir:data}" )
    private String dataDir = null;

    @Value( "${kinesis.consumer.region:us-east-1}" )
    private String kinesisConsumerRegion = "us-east-1";

    @Value( "${kinesis.relay.region:us-east-1}" )
    private String kinesisRelayRegion = "us-east-1";

    @Value( "${kinesis.relay.rbacEnabled:false}" ) private Boolean kinesisRelayRbacEnabled = false;

    @Value( "${kinesis.relay.account:}" ) private String kinesisRelayAccount;

    @Value( "${kinesis.relay.role:}" ) private String kinesisRelayRole;

    /**
     * Config server properties
     */
    @Value( "${configServer.enabled:false}" ) private boolean configServerEnabled;

    @Value( "${configServer.registrationSeconds:30}" ) private int configServerRegistrationSeconds;

    @Value( "${configServer.baseUrl:http://localhost:8081}" ) private String configServerBaseUrl;

    @Value( "${configServer.infrastructure:unknownInfra}" ) private String configServerInfrastructure;

    @Value( "${configServer.processName:unknownProcessName}" ) private String configServerProcessName;

    @Value( "${configServer.processInstance:unknownProcessInstance}" ) private String configServerProcessInstance;

    @Value( "${configServer.backupFilePath:config/config-server-state.json}" ) private String backupFilePath;

    @Value("${metrics.store.sync.secondary.db:false}")
    private boolean syncSecondaryDb;

    @Value("${spring.profiles.active:prd}")
    private String activeProfile;

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    @ConditionalOnProperty( name = "configServer.enabled", havingValue = "true" )
    public ConfigServerUtil configServerUtil(ScheduledExecutorService s, RestTemplate restTemplate) throws IOException {
        ConfigServerUtil configServerUtil = new ConfigServerUtil(restTemplate, configServerBaseUrl, metricRegistry,
                String.format("%s.%s.%s", configServerInfrastructure, configServerProcessName,
                        configServerProcessInstance), backupFilePath);
        s.scheduleWithFixedDelay(() -> {
            try {
                configServerUtil.register();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }, 15, configServerRegistrationSeconds, TimeUnit.SECONDS);
        return configServerUtil;
    }

    @Bean( name = "dataPointSinkRelay" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    Relay relay( ScheduledExecutorService s,
                 @Autowired(required = false) ConfigServerUtil configServerUtil )
    {
        File rulesFile = locateConfigFile( serviceDir, relayRulesFile );
        Relay r = new Relay( metricRegistry, "relay", rulesFile, destQueue, destBatchSize, refreshIntervalInMillis,
                        destConfigDir, maxWaitTimeInSecs, kinesisRelayRegion, relayRulesSrc, relayCacheEnabled, configServerUtil,
                        kinesisRelayRbacEnabled, kinesisRelayAccount, kinesisRelayRole);
        s.scheduleWithFixedDelay( r::reload, 15, 30, TimeUnit.SECONDS );
        return r;
    }

    @Bean( name = "auditLogRelay" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    Relay auditLog( ScheduledExecutorService s,
                    @Autowired(required = false) ConfigServerUtil configServerUtil )
    {
        File rulesFile = locateConfigFile( serviceDir, auditRulesFile );
        Relay r = new Relay( metricRegistry, "audit", rulesFile, destQueue, destBatchSize, refreshIntervalInMillis,
                        destConfigDir, maxWaitTimeInSecs, kinesisRelayRegion, auditRulesSrc, false, configServerUtil,
                        kinesisRelayRbacEnabled, kinesisRelayAccount, kinesisRelayRole);
        s.scheduleWithFixedDelay( r::reload, 15, 30, TimeUnit.SECONDS );
        return r;
    }

    @Bean
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    PointFilter pointFilter( NameUtils nameUtils )
    {
        return new PointFilter( metricRegistry, "pointFilter", maxLen, maxAge, maxFutureAge, nameUtils,
                        dupPointCacheMaxSize, dupPointCacheExpireInMin,
                        new Quota( errLogQuotaMax, errLogQuotaResetAfter ) );
    }

    @Bean( name = "pointBlacklist" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    MetricList pointBlacklist( ScheduledExecutorService s,
            @Autowired(required = false) ConfigServerUtil configServerUtil)
    {
        MetricList bs = new MetricList( metricRegistry, "blacklist", locateConfigFile( serviceDir, blacklistConfigFile ),
                metricListConfigSrc, configServerUtil );
        s.scheduleWithFixedDelay( bs::reload, 10, 30, TimeUnit.SECONDS );
        return bs;
    }

    @Bean( name = "queryBlacklist" ) MetricList queryBlacklist( ScheduledExecutorService s,
            @Autowired(required = false) ConfigServerUtil configServerUtil )
    {
        MetricList bs = new MetricList( metricRegistry, "queryBlacklist",
                        locateConfigFile( serviceDir, queryBlacklistConfigFile ), metricListConfigSrc, configServerUtil );
        s.scheduleWithFixedDelay( bs::reload, 10, 30, TimeUnit.SECONDS );
        return bs;
    }

    @Bean( name = "pointAllowOnlyList" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    MetricList pointAllowOnlyList( ScheduledExecutorService s,
            @Autowired(required = false) ConfigServerUtil configServerUtil )
    {
        MetricList metricList = new MetricList( metricRegistry, "allowOnly",
                locateConfigFile( serviceDir, allowOnlyMetricsConfigFile ), metricListConfigSrc, configServerUtil );
        s.scheduleWithFixedDelay( metricList::reload, 10, 30, TimeUnit.SECONDS );
        return metricList;
    }

    @Bean @DependsOn( "stringsCache" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    PointProcessor pointProcessor(
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

    @Bean( name = "recoveryPointProcessor" ) @DependsOn( "stringsCache" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    PointProcessor recoveryPointProcessor(
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

    @Bean
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    InputQueue inputQueue( PointProcessor pointProcessor )
    {
        return new InputQueue( metricRegistry, "input-queue-consumer", pointProcessor, aggregatorQueue,
                        relayQueueRejectPolicy, batchSize, emptyQueuePauseMillis );
    }

    @Bean( name = "datapoint_sink" )
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    Consumer<DataPoints> dataPointSink( @Qualifier( "dataPointSinkRelay" ) Relay r )
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

    @Autowired
    private KinesisAsyncClient kinesisAsyncClient;

    @Autowired
    private DynamoDbAsyncClient dynamoDbAsyncClient;

    @Autowired
    private CloudWatchAsyncClient cloudWatchAsyncClient;

    @Bean
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    Consumers consumer( PointProcessor pointProcessor,
                              @Qualifier( "recoveryPointProcessor" ) PointProcessor recoveryPointProcessor,
                              ScheduledExecutorService s, KinesisConfig kinesisConfig, NamespaceCounter nsCounter )
    {
        if ( kinesisConfig.isKinesisConsumerEnabled() )
        {
            File rulesFile = locateConfigFile( serviceDir, consumerRulesFile );
            Consumers consumer = new Consumers( metricRegistry, pointProcessor, recoveryPointProcessor, rulesFile,
                    kinesisConfig, checkPointMgr, kinesisConsumerRegion,
                    nsCounter, dataDir == null ? null : new File(dataDir, "index-name-sync"), activeProfile,
                    kinesisAsyncClient, dynamoDbAsyncClient, cloudWatchAsyncClient);
            s.scheduleWithFixedDelay( consumer::reload, 15, 30, TimeUnit.SECONDS );
            if (syncSecondaryDb) {
                s.scheduleWithFixedDelay( consumer::syncNamespaces, 60, 60, TimeUnit.SECONDS );
            }
            return consumer;
        }
        else
            return null;
    }

    @Bean Void stats( ScheduledExecutorService s,
                      @Autowired(required = false) @Qualifier( "dataPointSinkRelay" ) Relay r,
                      @Autowired(required = false) InputQueue a,
                      @Autowired(required = false) @Qualifier( "pointBlacklist" ) MetricList pbl,
                      @Qualifier( "queryBlacklist" ) MetricList qbl,
                      @Autowired(required = false) @Qualifier( "pointAllowOnlyList" ) MetricList pal,
                      NettyServer nettyServer,
                      @Autowired(required = false) @Qualifier( "auditLogRelay" ) Relay auditLog,
                      StringsCache strCache,
                      @Qualifier( "accumulator" ) Accumulator accu )
    {

        s.scheduleWithFixedDelay( () -> {
            if (a != null) a.dumpStats();
            if (pbl != null) pbl.dumpStats();
            if (qbl != null) qbl.dumpStats();
            if (pal != null) pal.dumpStats();
            if (r != null) r.dumpStats();
            if (auditLog != null) auditLog.dumpStats();
            if ( db != null ) db.dumpStats();
            if (nettyServer != null) nettyServer.dumpStats();
            if (strCache != null) strCache.dumpStats();
            if ( accu != null ) accu.dumpStats();
        }, 60, 60, TimeUnit.SECONDS );
        return null;
    }

    @Bean
    @ConditionalOnProperty(name = "rocksdb.readonly", havingValue = "false", matchIfMissing = true)
    Void checkExpiredNamespaces( ScheduledExecutorService s, NamespaceCounter ns )
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

    @Bean Void refreshStats( ScheduledExecutorService s, @Autowired(required = false) InputQueue a, @Qualifier( "accumulator" ) Accumulator accu )
    {
        s.scheduleWithFixedDelay( () -> {
            if (a != null) a.refreshStats();
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

    @Bean
    @ConditionalOnProperty(name = "carbonj.relay", havingValue = "true", matchIfMissing = true)
    NettyChannel lineProtocolChannel( NettyServer netty, InputQueue r )
    {
        lineProtocolTcpPort = ( lineProtocolTcpPort == -1 ) ? jettyPort + 2 : lineProtocolTcpPort;
        return netty.bind( lineProtocolTcpHost, lineProtocolTcpPort, new ChannelInitializer<SocketChannel>()
        {
            @Override public void initChannel(@SuppressWarnings("NullableProblems") SocketChannel ch )
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

    @Bean
    @ConditionalOnProperty(name = "carbonj.relay", havingValue = "true", matchIfMissing = true)
    NettyChannel udpLineProtocolChannel( NettyServer netty, InputQueue r )
    {
        lineProtocolUdpPort = ( lineProtocolUdpPort == -1 ) ? jettyPort + 2 : lineProtocolUdpPort;
        NettyChannel channel = netty.udpBind( lineProtocolUdpHost, lineProtocolUdpPort, udpBuff, udpMsgBuff,
                        new SimpleChannelInboundHandler<DatagramPacket>()
                        {
                            final LineProtocolHandler lp = new LineProtocolHandler( metricRegistry, r );

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

    @Bean
    @ConditionalOnProperty(name = "carbonj.relay", havingValue = "true", matchIfMissing = true)
    NettyChannel pickleProtocolChannel( NettyServer netty, InputQueue r )
    {
        return netty.bind( "0.0.0.0", jettyPort + 3, new ChannelInitializer<SocketChannel>()
        {
            @Override public void initChannel(@SuppressWarnings("NullableProblems") SocketChannel ch )
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

    @Bean CarbonjAdmin cjAdmin(@Autowired(required = false) InputQueue agg, NameUtils nameUtils )
    {
        return new CarbonjAdmin( agg, nameUtils, Optional.ofNullable( db ) );
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

    @Bean public ServletRegistrationBean<GraphiteMetricSearchServlet> graphiteMetricSearchServlet()
    {
        ServletRegistrationBean<GraphiteMetricSearchServlet> servletRegistration =
                        new ServletRegistrationBean<>( new GraphiteMetricSearchServlet(), "/metrics/*" );
        servletRegistration.setLoadOnStartup( 1 );
        return servletRegistration;
    }

    @Bean public ServletRegistrationBean<GraphiteSeriesDataServlet> graphiteSeriesDataServlet()
    {
        ServletRegistrationBean<GraphiteSeriesDataServlet> servletRegistration =
                        new ServletRegistrationBean<>( new GraphiteSeriesDataServlet(), "/render/*" );
        servletRegistration.setLoadOnStartup( 1 );
        return servletRegistration;
    }

    @Bean public ServletRegistrationBean<GraphiteSeriesDataTestServlet> graphiteSeriesDataTestServlet()
    {
        ServletRegistrationBean<GraphiteSeriesDataTestServlet> servletRegistration =
                        new ServletRegistrationBean<>( new GraphiteSeriesDataTestServlet(), "/render-test/*" );
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

                // new jetty 11 uses a slf4j log writer for performance reasons:
                // https://stackoverflow.com/questions/68737248/how-to-override-request-logging-mechanism-in-jetty-11-0-6
                RequestLog ncsaLog = new CustomRequestLog( jettyLogfilePath,  CustomRequestLog.EXTENDED_NCSA_FORMAT);
                server.setRequestLog(ncsaLog);
            } );
        }
    }
}
