/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.db.TimeSeriesStore;
import com.demandware.carbonj.service.db.TimeSeriesStoreImpl;
import com.demandware.carbonj.service.db.model.DataPointStore;
import com.demandware.carbonj.service.db.model.Metric;
import com.demandware.carbonj.service.db.model.MetricIndex;
import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.util.DatabaseMetrics;
import com.demandware.carbonj.service.db.util.SystemTime;
import com.demandware.carbonj.service.events.NoOpLogger;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import net.razorvine.pickle.PickleException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class _GraphiteSeriesDataServlet {
    @TempDir
    public File tempFolder;
    @Autowired
    private TimeSeriesStore timeSeriesStore;

    @Test
    @Disabled("Used only for generating pickle stream of each series. Good for method profiling.")
    public void testPickleStream()
            throws IOException {
        Preconditions.checkNotNull(timeSeriesStore);

        String target = "test";
        int now = SystemTime.nowEpochSecond();
        String from = String.valueOf(now - 24 * 60 * 60 + 120);
        String until = String.valueOf(now);

        // Streaming without creating series list
        final File pickleStreamOutput = new File(tempFolder, "pickleStream");
        FileOutputStream allStream = new FileOutputStream(pickleStreamOutput);

        GraphitePickler seriesStream = new GraphitePickler(false, allStream);
        timeSeriesStore.streamSeriesData(
                new Query(target, Integer.parseInt(from), Integer.parseInt(until), now, System.currentTimeMillis()),
                seriesStream);
        seriesStream.close();
        allStream.close();
        System.out.println(pickleStreamOutput.length());
    }

    @Test
    @Disabled("Used only for generating pickle stream while generating a large series list. Good for method profiling.")
    public void testPickleStreamWhenCreatingSeriesList()
            throws IOException {
        Preconditions.checkNotNull(timeSeriesStore);

        String target = "test";
        int now = SystemTime.nowEpochSecond();
        String from = String.valueOf(now - 24 * 60 * 60 + 120);
        String until = String.valueOf(now);

        // Streaming via series list
        final File pickleOutput = new File(tempFolder, "pickleOutput");
        FileOutputStream someStream = new FileOutputStream(pickleOutput);
        List<Series> series =
                timeSeriesStore.fetchSeriesData(
                        new Query(target, Integer.parseInt(from), Integer.parseInt(until), now, System.currentTimeMillis()));
        GraphitePickler someStreamingPickler = new GraphitePickler(false, someStream);
        someStreamingPickler.pickleSeriesList(series);
        someStream.close();
        System.out.println(pickleOutput.length());
    }

    @Test
    public void testPickleSeriesOutputStreamIdentical()
            throws PickleException, IOException {
        Preconditions.checkNotNull(timeSeriesStore);

        String target = "test";
        int now = SystemTime.nowEpochSecond();
        String from = String.valueOf(now - 24 * 60 * 60 + 120);
        String until = String.valueOf(now);

        // Streaming without creating series list
        final File pickleStreamOutput = new File(tempFolder, "pickleStream");
        FileOutputStream allStream = new FileOutputStream(pickleStreamOutput);

        GraphitePickler seriesStream = new GraphitePickler(false, allStream);
        timeSeriesStore.streamSeriesData(
                new Query(target, Integer.parseInt(from), Integer.parseInt(until), now, System.currentTimeMillis()),
                seriesStream);
        seriesStream.close();
        allStream.close();
        System.out.println(pickleStreamOutput.length());

        // Streaming via series list
        final File pickleOutput = new File(tempFolder, "pickleOutput");
        FileOutputStream someStream = new FileOutputStream(pickleOutput);
        List<Series> series =
                timeSeriesStore.fetchSeriesData(new Query(target, Integer.parseInt(from), Integer.parseInt(until), now, System.currentTimeMillis()));
        GraphitePickler someStreamingPickler = new GraphitePickler(false, someStream);
        someStreamingPickler.pickleSeriesList(series);
        someStream.close();
        System.out.println(pickleOutput.length());

        String streamResult = Files.asCharSource(pickleStreamOutput, Charsets.UTF_8).read();

        String someStreamResult = Files.asCharSource(pickleStreamOutput, Charsets.UTF_8).read();

        assertEquals(streamResult, someStreamResult);
    }

    @Configuration
    public static class cfg {
        private final int batchedSeriesSize = 10;
        private final int nTaskThreads = 10;
        private final int threadBlockingQueueSize = 1000;
        private final boolean batchedSeriesRetrieval = true;
        private final boolean dumpIndex = false;
        private final String dumpIndexFile = "index-data.out";
        private final int TEST_METRICS_SIZE = 100;

        @Bean
        MetricIndex metricIndex() {
            MetricIndex nameIndex = Mockito.mock(MetricIndex.class);

            when(nameIndex.selectRandomMetric()).thenReturn(new Metric("test", 1, null, null, null));

            List<Metric> metrics = new ArrayList<>();

            for (int i = 0; i < TEST_METRICS_SIZE; ++i) {
                Metric metric = spy(new Metric("test" + i, i, null, null, null));
                when(metric.isLeaf()).thenReturn(true);
                metrics.add(metric);
            }

            when(nameIndex.findMetrics("test")).thenReturn(metrics);

            return nameIndex;
        }

        @Bean
        DataPointStore pointStore() {
            DataPointStore pointStore = Mockito.mock(DataPointStore.class);

            List<Double> values = new ArrayList<>();
            for (int i = 0; i < 1; ++i) {
                for (double d = 0.0; d < 1440.0; d++) {
                    values.add(d + i);
                }
            }

            when(pointStore.getSeries(any(Metric.class), anyInt(), anyInt(), anyInt())).then(
                    i -> new Series(any(Metric.class).name, 0, 1440, 1, values));

            return pointStore;
        }

        @Bean
        DatabaseMetrics dbMetrics(MetricRegistry metricRegistry) {
            return new DatabaseMetrics( metricRegistry);
        }

        @Bean MetricRegistry metricRegistry(){
            return new MetricRegistry();
        }

        @Bean
        TimeSeriesStore store(MetricRegistry metricRegistry, MetricIndex nameIndex, DataPointStore pointStore, DatabaseMetrics dbMetrics) {
            return new TimeSeriesStoreImpl(metricRegistry, nameIndex, new NoOpLogger<>(), TimeSeriesStoreImpl.newMainTaskQueue(nTaskThreads,
                    threadBlockingQueueSize), TimeSeriesStoreImpl.newMainTaskQueue(1,
                    2), TimeSeriesStoreImpl.newSerialTaskQueue(10), pointStore, dbMetrics,
                    batchedSeriesRetrieval,
                    batchedSeriesSize, dumpIndex, new File(dumpIndexFile), 10, "doesntexist.conf", false);
        }

    }
}
