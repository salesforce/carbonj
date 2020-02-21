/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.sfcc.um.metrics_reporter.reporter;

import com.codahale.metrics.*;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.sfcc.um.metrics_reporter.metricset.OperatingSystemGaugeSet;
import com.sfcc.um.metrics_reporter.transport.GraphiteTransport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class GraphiteReporter extends ScheduledReporter
{
    /**
     * Returns a new {@link Builder} for {@link GraphiteReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link GraphiteReporter}
     */
    public static Builder forRegistry( MetricRegistry registry )
    {
        return new Builder( registry );
    }

    private final Logger LOG = LogManager.getLogger( GraphiteReporter.class );

    private final GraphiteTransport graphite;

    private final Clock clock;

    private final String prefix;

    /**
     * A builder for {@link GraphiteReporter} instances. Defaults to not using a prefix, using the default clock,
     * converting rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder
    {
        private final MetricRegistry registry;

        private Clock clock;

        private String prefix;

        private TimeUnit rateUnit;

        private TimeUnit durationUnit;

        private MetricFilter filter;

        private Builder( MetricRegistry registry )
        {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock( Clock clock )
        {
            this.clock = clock;
            return this;
        }

        /**
         * @param prefix the prefix for all metric names
         * @return {@code this}
         */
        public Builder withPrefix( String prefix )
        {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo( TimeUnit rateUnit )
        {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo( TimeUnit durationUnit )
        {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter( MetricFilter filter )
        {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link GraphiteReporter} with the given properties, sending metrics using the given
         * Graphite client.
         *
         * @param transport a means to talk to graphite
         * @return a {@link GraphiteReporter}
         */
        public GraphiteReporter build( GraphiteTransport transport )
        {
            return new GraphiteReporter( registry, transport, clock, prefix, rateUnit, durationUnit, filter );
        }
    }

    private void registerJVMMetrics( MetricRegistry registry )
    {
        /*
        * Cannot add JvmAttributeGaugeSet because multiple versions of dropwizard metrics
        * are present on classpath and I am not able to remove the older versions
        * */
        registry.register( "jvm.attributes", new JvmAttributeGaugeSet(  ) );
        registry.register( "jvm.gc", new GarbageCollectorMetricSet(  ) );
        registry.register( "jvm.memory", new MemoryUsageGaugeSet(  ) );
        registry.register( "jvm.threads", new ThreadStatesGaugeSet(  ) );
    }

    private void registerOSMetrics( MetricRegistry registry )
    {
        registry.register( "os", new OperatingSystemGaugeSet(  ) );
    }

    private void registerMonitoringMetrics( MetricRegistry registry )
    {
        registerJVMMetrics( registry );
        registerOSMetrics( registry );
    }

    private GraphiteReporter( MetricRegistry registry,
                              GraphiteTransport transport,
                              Clock clock,
                              String prefix,
                              TimeUnit rateUnit,
                              TimeUnit durationUnit,
                              MetricFilter filter )
    {
        super( registry, "graphite-reporter", filter, rateUnit, durationUnit );
        registerMonitoringMetrics( registry );
        this.graphite = transport;
        this.clock = clock;
        this.prefix = prefix;
    }

    @Override
    public void report( SortedMap<String, Gauge> gauges,
                        SortedMap<String, Counter> counters,
                        SortedMap<String, Histogram> histograms,
                        SortedMap<String, Meter> meters,
                        SortedMap<String, Timer> timers )
    {
        final long timestamp = clock.getTime() / 1000;

        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( "Submitting metrics to graphite '{}' ", graphite );
        }

        // oh it'd be lovely to use Java 7 here
        try
        {
            graphite.open();

            for ( Map.Entry<String, Gauge> entry : gauges.entrySet() )
            {
                reportGauge( entry.getKey(), entry.getValue(), timestamp );
            }

            for ( Map.Entry<String, Counter> entry : counters.entrySet() )
            {
                reportCounter( entry.getKey(), entry.getValue(), timestamp );
            }

            for ( Map.Entry<String, Histogram> entry : histograms.entrySet() )
            {
                reportHistogram( entry.getKey(), entry.getValue(), timestamp );
            }

            for ( Map.Entry<String, Meter> entry : meters.entrySet() )
            {
                reportMetered( entry.getKey(), entry.getValue(), timestamp );
            }

            for ( Map.Entry<String, Timer> entry : timers.entrySet() )
            {
                reportTimer( entry.getKey(), entry.getValue(), timestamp );
            }
        }
        catch ( IOException e )
        {
            LOG.warn( "Unable to report to Graphite '{}'", graphite, e );
        }
        finally
        {
            try
            {
                graphite.close();
            }
            catch ( IOException e )
            {
                LOG.debug( "Error disconnecting from Graphite '{}'", graphite, e );
            }
        }
    }

    private void reportTimer( String name, Timer timer, long timestamp )
                    throws IOException
    {
        final Snapshot snapshot = timer.getSnapshot();

        // NOTE: some metrics are not published since we can calculate them in Graphite again
        graphite.send( prefix( name, "max" ), format( convertDuration( snapshot.getMax() ) ), timestamp );
        graphite.send( prefix( name, "mean" ), format( convertDuration( snapshot.getMean() ) ), timestamp );
        graphite.send( prefix( name, "min" ), format( convertDuration( snapshot.getMin() ) ), timestamp );
        graphite.send( prefix( name, "stddev" ), format( convertDuration( snapshot.getStdDev() ) ), timestamp );
        graphite.send( prefix( name, "p95" ), format( convertDuration( snapshot.get95thPercentile() ) ), timestamp );

        reportMetered( name, timer, timestamp );
    }

    private void reportMetered( String name, Metered meter, long timestamp )
                    throws IOException
    {
        // NOTE: some metrics are not published since we can calculate them in Graphite again
        graphite.send( prefix( name, "count" ), format( meter.getCount() ), timestamp );
        graphite.send( prefix( name, "m1_rate" ), format( convertRate( meter.getOneMinuteRate() ) ), timestamp );
    }

    private void reportHistogram( String name, Histogram histogram, long timestamp )
                    throws IOException
    {
        // NOTE: some metrics are not published since we can calculate them in Graphite again
        final Snapshot snapshot = histogram.getSnapshot();
        graphite.send( prefix( name, "count" ), format( histogram.getCount() ), timestamp );
        graphite.send( prefix( name, "max" ), format( snapshot.getMax() ), timestamp );
        graphite.send( prefix( name, "mean" ), format( snapshot.getMean() ), timestamp );
        graphite.send( prefix( name, "min" ), format( snapshot.getMin() ), timestamp );
        graphite.send( prefix( name, "stddev" ), format( snapshot.getStdDev() ), timestamp );
        graphite.send( prefix( name, "p95" ), format( snapshot.get95thPercentile() ), timestamp );
    }

    private void reportCounter( String name, Counter counter, long timestamp )
                    throws IOException
    {
        graphite.send( prefix( name, "count" ), format( counter.getCount() ), timestamp );
    }

    private void reportGauge( String name, Gauge gauge, long timestamp )
                    throws IOException
    {
        String value = null;
        try
        {
            // use try / catch as gauge implementations may throw
            value = format( gauge.getValue() );
        }
        catch ( Exception e )
        {
            LOG.warn( "Error retrieving value for gauge metric '{}'", name, e );
        }

        if ( value != null )
        {
            graphite.send( prefix( name ), value, timestamp );
        }
    }

    private String format( Object o )
    {
        if ( o instanceof Float )
        {
            return format( ( (Float) o ).doubleValue() );
        }
        else if ( o instanceof Double )
        {
            return format( ( (Double) o ).doubleValue() );
        }
        else if ( o instanceof Byte )
        {
            return format( ( (Byte) o ).longValue() );
        }
        else if ( o instanceof Short )
        {
            return format( ( (Short) o ).longValue() );
        }
        else if ( o instanceof Integer )
        {
            return format( ( (Integer) o ).longValue() );
        }
        else if ( o instanceof Long )
        {
            return format( ( (Long) o ).longValue() );
        }
        return null;
    }

    private String prefix( String... components )
    {
        return MetricRegistry.name( prefix, components );
    }

    private String format( long n )
    {
        return Long.toString( n );
    }

    private String format( double v )
    {
        // the Carbon plaintext format is pretty underspecified, but it seems like it just wants
        // US-formatted digits
        return String.format( Locale.US, "%2.2f", v );
    }
}
