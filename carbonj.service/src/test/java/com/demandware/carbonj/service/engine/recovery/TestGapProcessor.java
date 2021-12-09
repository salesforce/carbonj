/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.Accumulator;
import com.demandware.carbonj.service.accumulator.DefaultSlotStrategy;
import com.demandware.carbonj.service.engine.*;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.OngoingStubbing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class TestGapProcessor {

    private MetricRegistry metricRegistry = new MetricRegistry();

    private int sequenceNumber = 0;

    @Before
    public void setUp() {
        sequenceNumber = 0;
    }

    @Test
    public void testBasic() throws Exception {

        long baseTime = 1521181414065L;                                            // Fri Mar 16 02:23:34 EDT 2018
        long gapEndTime = baseTime - TimeUnit.MINUTES.toMillis(30);       // Fri Mar 16 01:53:34 EDT 2018
        long gapStartTime = gapEndTime - TimeUnit.MINUTES.toMillis(14);   // Fri Mar 16 01:39:34 EDT 2018

        verify(gapStartTime, gapEndTime, 6, 5, 5, 5);
    }

    @Test
    public void testFiltered() throws Exception {

        long baseTime = 1521181414065L;                                            // Fri Mar 16 02:23:34 EDT 2018
        long gapEndTime = baseTime - TimeUnit.MINUTES.toMillis(30);       // Fri Mar 16 01:53:34 EDT 2018
        long gapStartTime = gapEndTime - TimeUnit.MINUTES.toMillis(14);   // Fri Mar 16 01:39:34 EDT 2018

        verify(gapStartTime, gapEndTime, 8, 1, 4, 4);
    }

    private void verify(long gapStartTime, long gapEndTime, int expectedPointProcessorSubmissions, int firstSubSize,
                        int lastSubSize, int simStartOffsetInMins) throws Exception {

        System.out.println("Provided gap start time: " + new Date(gapStartTime) + " gap end time: " + new Date(gapEndTime));

        List<ShardInfo> mockShardInfos = Arrays.asList(new ShardInfoImpl("1"), new ShardInfoImpl("2"));

        Gap gap = new GapImpl(new Date(gapStartTime), new Date(gapEndTime));
        Map<String, DataPoints> seqNumToDataPoints = new HashMap<>();
        KinesisStream mockKinesisStream = getMockKinesisStream(mockShardInfos, seqNumToDataPoints, gapStartTime, gapEndTime, simStartOffsetInMins);

        List<List<DataPoint>> dataPointsList = new ArrayList<>();
        PointProcessor mockPointProcessor = getMockPointProcessor(dataPointsList);
        DataPointCodec mockCodec = getMockCodec(seqNumToDataPoints);
        GapProcessor gapProcessor = new GapProcessor(metricRegistry, gap, mockKinesisStream, mockPointProcessor, 5, mockCodec);
        gapProcessor.process();

        int noOfSubmissions = dataPointsList.size();
        Assert.assertEquals(expectedPointProcessorSubmissions, noOfSubmissions);  // number of times point processor should have been called.

        long prevTimeStamp = -1;
        for (List<DataPoint> dataPoints: dataPointsList) {
            long timeStamp = dataPoints.get(0).ts * 1000L;
            Assert.assertTrue(timeStamp >= prevTimeStamp);
            prevTimeStamp = timeStamp;
        }

        // first time stamp in here should be filtered.  and it should be Fri Mar 16 01:39:39 EDT 2018
        List<DataPoint> firstProcessed = dataPointsList.get(0);
        Assert.assertEquals(firstSubSize, firstProcessed.size());
        // Assert.assertEquals(, firstProcessed.get(0).ts);

        for (int i = 2; i < noOfSubmissions - 4; i++) {
            Assert.assertEquals(5, dataPointsList.get(i).size());
        }

        List<DataPoint> lastProcessed = dataPointsList.get(noOfSubmissions - 1);
        Assert.assertEquals(lastSubSize, lastProcessed.size());
        // Assert.assertEquals(, lastProcessed.get(2).ts);
    }

    private DataPointCodec getMockCodec(Map<String, DataPoints> seqNumToDataPoints) {
        return new MockCodec(seqNumToDataPoints);
    }

    private PointProcessor getMockPointProcessor(List<List<DataPoint>> processedDataPoints) {
        return new MockPointProcessor(processedDataPoints);
    }

    private KinesisStream getMockKinesisStream(List<ShardInfo> mockShards, Map<String, DataPoints> seqNumToDataPoints,
                                               long gapStartTime, long gapEndTime, int simStartOffSetInMins) throws Exception {
        KinesisStream kinesisStream = mock(KinesisStream.class);

        when(kinesisStream.getShards()).thenReturn(new HashSet<>(mockShards));

        when(kinesisStream.getShardIterator(any(ShardInfo.class), any(Date.class))).thenReturn("1");

        long simulationStartTime = gapStartTime - TimeUnit.MINUTES.toMillis(simStartOffSetInMins) + TimeUnit.SECONDS.toMillis(5);  //Fri Mar 16 01:34:39 EDT 2018
        long simulationEndTime = gapEndTime + TimeUnit.MINUTES.toMillis(10);     //Fri Mar 16 02:03:34 EDT 2018

        for (ShardInfo mockShard : mockShards) {
            OngoingStubbing<RecordAndIterator> whenTemplate = when(kinesisStream.getNextRecord(eq(mockShard),
                    any(String.class), any()));
            for (long recordStartTime = simulationStartTime; recordStartTime < simulationEndTime ; recordStartTime += TimeUnit.MINUTES.toMillis(5) ) {
                long recordEndTime = recordStartTime + TimeUnit.MINUTES.toMillis(5);
                DataPoints dataPoints = generateDataPoints(recordStartTime, recordEndTime);
                int recordSeqNum = sequenceNumber++;
                String sequenceNumberStr = Integer.toString(recordSeqNum);
                seqNumToDataPoints.put(sequenceNumberStr, dataPoints);
                Record record = new Record()
                        .withData(ByteBuffer.wrap(sequenceNumberStr.getBytes(StandardCharsets.US_ASCII)))
                        .withSequenceNumber(sequenceNumberStr)
                        .withApproximateArrivalTimestamp(new Date(recordEndTime));
                RecordAndIterator recordAndIterator = new RecordAndIterator(record, sequenceNumberStr);
                whenTemplate = whenTemplate.thenReturn(recordAndIterator);

                List<DataPoint> dataPointList = dataPoints.getDataPoints();
                long firstRecordTime = dataPointList.get(0).ts * 1000L;
                long lastRecordTime = dataPointList.get(dataPointList.size() - 1).ts * 1000L;
                System.out.println("generated data points between " + new Date(firstRecordTime) + " - " + new Date(lastRecordTime) + " for " + mockShard);
            }
        }

        return kinesisStream;
    }

    private DataPoints generateDataPoints(long startTimeStamp, long endTimeStamp) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (long timeStamp = startTimeStamp; timeStamp < endTimeStamp; timeStamp += TimeUnit.MINUTES.toMillis(1)) {
            DataPoint dataPoint = new DataPoint("metric1", 1, (int) (timeStamp/1000));
            dataPoints.add(dataPoint);
        }
        return new DataPoints(dataPoints, startTimeStamp);
    }

    private DataPoints generateDataPoints(int... timeStamps) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (int timeStamp: timeStamps) {
            DataPoint dataPoint = new DataPoint("metric1", 1, timeStamp);
            dataPoints.add(dataPoint);
        }
        return new DataPoints(dataPoints, timeStamps[0]);
    }

    private static class MockPointProcessor implements PointProcessor {

        private List<List<DataPoint>> processedPoints;
        private Accumulator accumulator;

        private MockPointProcessor(List<List<DataPoint>> processedPoints) {
            this.processedPoints = processedPoints;
            accumulator = mock(Accumulator.class);
            when(accumulator.getSlotStrategy()).thenReturn(new DefaultSlotStrategy());
        }

        @Override
        public void process(List<DataPoint> points) {
            processedPoints.add(points);
        }

        @Override
        public void drain() {}

        @Override
        public void close() {}

        @Override
        public Accumulator getAccumulator() {
            return accumulator;
        }

        @Override
        public void flushAggregations(boolean force) {}

        @Override
        public void dumpStats() {}
    }

    private static class MockCodec implements DataPointCodec {

        private final Map<String, DataPoints> seqNumToDataPoints;

        MockCodec(Map<String, DataPoints> seqNumToDataPoints) {
            this.seqNumToDataPoints = seqNumToDataPoints;
        }

        @Override
        public DataPoints decode(byte[] data) {
            String seqNum = new String(data, StandardCharsets.US_ASCII);
            return seqNumToDataPoints.get(seqNum);
        }

        @Override
        public byte[] encode(DataPoints dataPoints) {
            return new byte[0];
        }
    }

    @Test
    public void testDataPointsInfo() {
        DataPoints dataPoints = generateDataPoints(9, 5, 4, 6, 7, 8);
        GapProcessor.DataPointsInfo dataPointsInfo = new GapProcessor.DataPointsInfo(dataPoints, new ShardInfoImpl("1"), 1);
        Assert.assertEquals(4, dataPointsInfo.minTs);
        Assert.assertEquals(9, dataPointsInfo.maxTs);
    }
}
