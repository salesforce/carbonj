/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.destination;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.demandware.carbonj.service.engine.BlockingPolicy;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.InputQueueThreadFactory;
import com.demandware.carbonj.service.engine.kinesis.GzipDataPointCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;


public class KinesisDestination
        extends Destination
        implements LineProtocolDestination
{

    private static final Logger log = LoggerFactory.getLogger( KinesisDestination.class );

    private final AmazonKinesis kinesisClient;

    private final ArrayBlockingQueue<DataPoint> q;

    private final String streamName;

    private final int batchSize;

    private final ThreadPoolExecutor ex;
    private final int maxWaitTimeInSecs;

    private volatile boolean stop = false;

    private final Meter messagesSent;

    private final Meter messageRetryCounter;

    private final Histogram messageSize;

    private final Histogram dataPointsPerMessage;

    private final Histogram activeThreads;

    private final Timer producerTimer;

    private final Boolean kinesisRelayRbacEnabled;

    private final String kinesisRelayAccount;

    private final String kinesisRelayRegion;

    private final String kinesisRelayRole;

    public KinesisDestination(MetricRegistry metricRegistry, String type, int queueSize,
                              String streamName, int batchSize, int threadCount, int maxWaitTimeInSecs, String kinesisRelayRegion,
                              Boolean kinesisRelayRbacEnabled, String kinesisRelayAccount, String kinesisRelayRole)
    {
        super(metricRegistry,"dest." + type + "." + streamName);

        this.messagesSent = metricRegistry.meter
                (MetricRegistry.name( "kinesis", "messagesSent" ) );

        this.messageRetryCounter = metricRegistry.meter
                (MetricRegistry.name( "kinesis", "putRetry") );

        this.messageSize = metricRegistry.histogram(
                MetricRegistry.name( "kinesis", "messageSize" ) );

        this.dataPointsPerMessage = metricRegistry.histogram(
                MetricRegistry.name( "kinesis", "dataPointsPerMessage" ) );

        this.activeThreads = metricRegistry.histogram(
                MetricRegistry.name("kinesis", "producerActiveThreads" ) );

        Timer blockingTimer = metricRegistry.timer(
                MetricRegistry.name("kinesis", "producer"));

        this.producerTimer = metricRegistry.timer(
                MetricRegistry.name( "kinesis", "producerTimer" ) );


        this.maxWaitTimeInSecs = maxWaitTimeInSecs;

        this.kinesisRelayRbacEnabled = kinesisRelayRbacEnabled;
        this.kinesisRelayRegion = kinesisRelayRegion;
        this.kinesisRelayAccount = kinesisRelayAccount;
        this.kinesisRelayRole = kinesisRelayRole;

        if ( kinesisRelayRbacEnabled )
        {
            kinesisClient = AmazonKinesisClientBuilder.standard().withCredentials( buildCredentialsProvider(kinesisRelayRegion, kinesisRelayAccount, kinesisRelayRole ) )
                    .withRegion( kinesisRelayRegion ).build();
        }
        else
        {
            kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(kinesisRelayRegion).build();
        }

        this.streamName = streamName;
        this.batchSize = batchSize;

        q = new ArrayBlockingQueue<>( queueSize );
        ex = new ThreadPoolExecutor(threadCount, threadCount,24,
                TimeUnit.HOURS,new ArrayBlockingQueue<>(5 * threadCount),
                new InputQueueThreadFactory( "kinesis-producer-task-" ), new BlockingPolicy(blockingTimer) );
        this.start();
    }

    private static AWSCredentialsProvider buildCredentialsProvider(String kinesisRelayRegion, String kinesisRelayAccount, String kinesisRelayRole)
    {
        String roleArn = "arn:aws:iam::" + kinesisRelayAccount + ":role/" + kinesisRelayRole;
        String roleSessionName = "assumedRole";

        final AWSCredentialsProvider credentialsProvider;

        AWSSecurityTokenService stsClient =
                AWSSecurityTokenServiceAsyncClientBuilder.standard().withRegion( kinesisRelayRegion ).build();

        credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder( roleArn, roleSessionName )
                .withStsClient( stsClient ).withRoleSessionDurationSeconds( 3600 ).build();

        return credentialsProvider;
    }


    @Override
    public void accept(DataPoint dataPoint)
    {   // Accepts data points only after the stream is ready.
        if ( q.offer( dataPoint )  )
        {
            received.mark();
            return;
        }
        drop.mark();
        if ( log.isDebugEnabled() )
        {
            log.debug( "Dropped->" + this + ". Queue size " + q.size() + ". Total dropped " + drop.getCount() );
        }
    }

    @Override
    public void closeQuietly() {
        try
        {
            close();
        }
        catch ( Exception e )
        {
            log.error( "Failed to close kinesis destination " + streamName, e );
        }
    }

    public void close()
    {
        log.info( "Stopping Kinesis dest " +this );
        try
        {
            if ( null != ex )
            {
                stop = true;
                log.info("Stop flag set to true in kinesis Destination");
                ex.shutdown();
                //noinspection ResultOfMethodCallIgnored
                ex.awaitTermination(15, TimeUnit.SECONDS);
                log.info( "Kinesis Stream Dest stopped." );
            }
        }
        catch (InterruptedException e)
        {
            log.error(e.getMessage(), e);
        }
    }

    private Runnable taskBuilder(List<DataPoint> points, Timer.Context timerContext) {
        return new KinesisProducerTask(metricRegistry, kinesisClient, streamName, points, sent, drop, messagesSent, messageSize,
                messageRetryCounter, timerContext, dataPointsPerMessage, new GzipDataPointCodec());
    }

    @Override
    public void run() {

        while (true)
        {
            if (stop) return;
            /* Reading data points from queue and submitting as a task to executor service */
            List<DataPoint> buf = new ArrayList<>(batchSize);
            int remainingBatchSize = batchSize;
            final Timer.Context timerContext = producerTimer.time();
            long maxWaitTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(maxWaitTimeInSecs);
            int noOfDataPoints = 0;
            try {
                while (remainingBatchSize > 0 && System.currentTimeMillis() < maxWaitTime) {
                    if (q.drainTo(buf, remainingBatchSize) == 0) {
                        DataPoint p = q.poll(100, TimeUnit.MILLISECONDS);
                        if (p != null) {
                            buf.add(p);
                        }
                    }
                    remainingBatchSize = batchSize - buf.size();
                }
                noOfDataPoints = buf.size();
                if (noOfDataPoints > 0) {
                    Runnable task = taskBuilder(buf, timerContext);
                    ex.submit(task);
                    activeThreads.update(ex.getActiveCount());
                }
            } catch (Throwable  e) {
                log.error(e.getMessage(), e);
                drop.mark(noOfDataPoints);
            }
        }
    }

    @Override
    public Consumer<DataPoint> andThen(@SuppressWarnings("NullableProblems") Consumer<? super DataPoint> after) {
        //noinspection DataFlowIssue
        return null;
    }

}
