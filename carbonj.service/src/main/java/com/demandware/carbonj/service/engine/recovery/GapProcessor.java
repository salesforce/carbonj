/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.accumulator.SlotStrategy;
import com.demandware.carbonj.service.engine.DataPoint;
import com.demandware.carbonj.service.engine.PointProcessor;
import com.demandware.carbonj.service.engine.kinesis.DataPointCodec;
import com.demandware.carbonj.service.engine.kinesis.DataPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GapProcessor {

    private static final Logger log = LoggerFactory.getLogger(GapProcessor.class);

    private static final int NUM_RETRIES = 3;
    private static final long BACKOFF_TIME_IN_MILLIS = 1000;

    private final Meter retries;
    private final Meter dropped;
    private final Counter duration;

    private final Date startTimeStamp;
    private final Date endTimeStamp;
    private final KinesisStream kinesisStream;
    private final PointProcessor pointProcessor;
    private final long idleTimeInMillis;
    private final DataPointCodec codec;

    GapProcessor(MetricRegistry metricRegistry, Gap gap, KinesisStream kinesisStream, PointProcessor pointProcessor,
                 long idleTimeInMillis, DataPointCodec codec) {

        this.kinesisStream = kinesisStream;
        this.pointProcessor = pointProcessor;
        this.idleTimeInMillis = idleTimeInMillis;
        this.codec = codec;

        this.retries = metricRegistry.meter(MetricRegistry.name("gapProcessor","retry"));
        this.dropped = metricRegistry.meter(MetricRegistry.name("gapProcessor","dropped"));
        this.duration = metricRegistry.counter(MetricRegistry.name("gapProcessor","duration"));

        if (pointProcessor.getAccumulator() != null) {
            SlotStrategy slotStrategy = pointProcessor.getAccumulator().getSlotStrategy();

            int slotTs = slotStrategy.getSlotTs((int) (gap.lastRecovered().getTime() / 1000));
            int startTs = slotStrategy.getStartTs(slotTs);
            this.startTimeStamp = new Date(startTs * 1000L);

            slotTs = slotStrategy.getSlotTs((int) (gap.endTime().getTime() / 1000));
            int endTs = slotStrategy.getEndTs(slotTs);
            this.endTimeStamp = new Date(endTs * 1000L);
        } else {
            this.startTimeStamp = gap.startTime();
            this.endTimeStamp = gap.endTime();
        }
    }

    public void process() throws InterruptedException {

        log.info("Recovery: Recovering gap: " + startTimeStamp + " - " + endTimeStamp);

        long startTime = System.nanoTime();

        long gapStartTimeInMillis = startTimeStamp.getTime();
        long gapEndTimeInMillis = endTimeStamp.getTime();

        Set<ShardInfo> shardInfos = kinesisStream.getShards();
        Map<ShardInfo, RecordTrackingInfo> shardToTrackingInfoMap = new HashMap<>();

        Queue<DataPointsInfo> queue = new PriorityQueue<>(Comparator.comparingLong(dp -> dp.arrivalTime));

        // first get the record for each of the shards and place this in the priority queue
        log.info("Fetching the first records for all shards..");

        for (ShardInfo shardInfo : shardInfos) {
            DataPointsInfo nextRecord = getNextRecord(shardToTrackingInfoMap, shardInfo);
            if (nextRecord != DataPointsInfo.EMPTY) {
                queue.add(nextRecord);
            }
            log.info(String.format("Recovery: fetched %s record", shardInfo));
        }

        log.info("Finished fetching the first records for all shards..");

        // take each record from the queue and check if it is not greater than end time.
        // if it is,  we are done.
        // else, process them.  after that,  get the next record from the shard and push it to the priority queue.
        while (!queue.isEmpty()) {
            DataPointsInfo dataPointsInfo = queue.poll();

            DataPoints dataPoints = dataPointsInfo.dataPoints;

            long recordMinTimeStamp = dataPointsInfo.minTs * 1000L;
            long recordMaxTimeStamp = dataPointsInfo.maxTs * 1000L;

            ShardInfo shardInfo = dataPointsInfo.shardInfo;

            if (log.isDebugEnabled())
            {
                log.debug( String.format( "Recovery: Processing %s : Record min timestamp: %tc", shardInfo,
                                new Date( recordMinTimeStamp ) ) );
            }
            if (recordMinTimeStamp > gapEndTimeInMillis) {  // finish processing...  we have processed all records in the gap.
                if (log.isDebugEnabled())
                {
                    log.debug( String
                                    .format( "Recovery: Finished processing %s : Received timestamp: %tc  End timestamp: %tc record",
                                                    shardInfo, new Date( recordMinTimeStamp ), endTimeStamp ) );
                }
                break;
            }

            boolean allDataPointsWithinGap = recordMinTimeStamp >= gapStartTimeInMillis &&
                    recordMaxTimeStamp <= gapEndTimeInMillis;

            if (!allDataPointsWithinGap) {

                boolean allRecordsOutsideGap = recordMaxTimeStamp < gapStartTimeInMillis;

                if (allRecordsOutsideGap) {
                    dataPoints = DataPoints.EMPTY;
                } else {
                    List<DataPoint> filteredDataPoints = new ArrayList<>();
                    for (DataPoint dataPoint : dataPoints.getDataPoints()) {
                        long tsInMillis = dataPoint.ts * 1000L;
                        if (tsInMillis < gapStartTimeInMillis) {
                            continue;
                        }
                        if (tsInMillis > gapEndTimeInMillis) {
                            continue;
                        }
                        if (log.isDebugEnabled() && dataPoint.name.contains("webadapter.bbdl.bbdl_prd")) {
                            log.debug("filteredDataPoints------------->>>>>>>>>>" + dataPoint);
                        }
                        filteredDataPoints.add(dataPoint);
                    }
                    if (log.isDebugEnabled())
                    {
                        log.debug(String
                                        .format( "Recovery: filtered submission %s : %tc record %d datapoints %d original",
                                                        shardInfo, new Date( recordMinTimeStamp ), filteredDataPoints.size(),
                                                        dataPointsInfo.dataPoints.getDataPoints().size() ) );
                    }
                    dataPoints = new DataPoints(filteredDataPoints, dataPoints.getTimeStamp());
                }
            }

            if (!dataPoints.getDataPoints().isEmpty()) {
                processRecordsWithRetries(dataPoints);
            }

            DataPointsInfo nextRecord = getNextRecord(shardToTrackingInfoMap, shardInfo);
            if (nextRecord != DataPointsInfo.EMPTY) {
                queue.add(nextRecord);
            }

            long currentTime = System.nanoTime();
            duration.inc(currentTime - startTime);
            startTime = currentTime;

            Thread.sleep(idleTimeInMillis);
        }

        duration.inc(System.nanoTime() - startTime);

        log.info("Exiting gap processor");
    }

    private DataPointsInfo getNextRecord(Map<ShardInfo, RecordTrackingInfo> shardToTrackingInfoMap,
                               ShardInfo shardInfo) throws InterruptedException {
        RecordTrackingInfo trackingInfo = shardToTrackingInfoMap.get(shardInfo);
        if (trackingInfo == null) {
            String shardIterator = kinesisStream.getShardIterator(shardInfo, startTimeStamp);
            trackingInfo = new RecordTrackingInfo(null, shardIterator);
        }

        RecordAndIterator recordAndIterator = kinesisStream.getNextRecord(shardInfo,
                trackingInfo.getShardIterator(), trackingInfo.getLastSequenceNumber());

        if (recordAndIterator == RecordAndIterator.EMPTY) {  // end of stream has been reached.
            shardToTrackingInfoMap.remove(shardInfo);
            return DataPointsInfo.EMPTY;
        }

        Record record = recordAndIterator.getRecord();
        String iterator = recordAndIterator.getIterator();

        shardToTrackingInfoMap.put(shardInfo, new RecordTrackingInfo(record.getSequenceNumber(), iterator));

        DataPoints dataPoints = codec.decode(record.getData().array());

        DataPointsInfo dataPointsInfo = new DataPointsInfo(dataPoints, shardInfo, record.getApproximateArrivalTimestamp().getTime());

        if (log.isDebugEnabled())
        {
            log.debug( String.format( "Recovery: fetched %s : %tc record; Record min timestamp: %tc", shardInfo, record.getApproximateArrivalTimestamp(), new Date(
                            dataPointsInfo.minTs * 1000L ) ) );
        }
        return dataPointsInfo;
    }

    private void processRecordsWithRetries(DataPoints dataPoints) {
        boolean processedSuccessfully = false;
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                processSingleRecord(dataPoints);
                processedSuccessfully = true;
                break;
            } catch (Throwable t) {
                log.error("Caught throwable while processing record "+ t.getMessage(), t);
            }

            retries.mark();

            // backoff if we encounter an exception.
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                log.error("Interrupted sleep: " + e.getMessage() ,e);
            }
        }

        if (!processedSuccessfully) {
            dropped.mark();
        }
    }

    private void processSingleRecord(DataPoints record) {
        pointProcessor.process(record.getDataPoints());
    }

    private static class RecordTrackingInfo {
        private final String lastSequenceNumber;
        private final String shardIterator;

        RecordTrackingInfo(String lastSequenceNumber, String shardIterator) {
            this.lastSequenceNumber = lastSequenceNumber;
            this.shardIterator = shardIterator;
        }

        String getLastSequenceNumber() {
            return lastSequenceNumber;
        }

        String getShardIterator() {
            return shardIterator;
        }
    }

    static class DataPointsInfo {

        static final DataPointsInfo EMPTY = new DataPointsInfo(null, null, -1, -1, -1);

        final DataPoints dataPoints;
        final ShardInfo shardInfo;
        final long arrivalTime;
        int minTs;
        int maxTs;

        DataPointsInfo(DataPoints dataPoints, ShardInfo shardInfo, long arrivalTime) {
            this.dataPoints = dataPoints;
            this.shardInfo = shardInfo;
            this.arrivalTime = arrivalTime;

            List<DataPoint> dataPointsList = dataPoints.getDataPoints();
            minTs = maxTs = dataPointsList.get(0).ts;
            if (log.isDebugEnabled() && dataPointsList.get(0).name.contains("webadapter.bbdl.bbdl_prd")) {
                log.debug("DataPointsInfo----------->>>>>>>>>>>>" + dataPointsList.get(0));
            }
            int size = dataPointsList.size();
            for (int i = 1; i < size; i++) {
                int ts = dataPointsList.get(i).ts;
                if (log.isDebugEnabled() && dataPointsList.get(i).name.contains("webadapter.bbdl.bbdl_prd")) {
                    log.debug("DataPointsInfo----------->>>>>>>>>>>>" + dataPointsList.get(i));
                }
                minTs = Math.min(minTs, ts);
                maxTs = Math.max(maxTs, ts);
            }
        }

        DataPointsInfo(DataPoints dataPoints, ShardInfo shardInfo, long arrivalTime, int minTs, int maxTs) {
            this.dataPoints = dataPoints;
            this.shardInfo = shardInfo;
            this.arrivalTime = arrivalTime;
            this.minTs = minTs;
            this.maxTs = maxTs;
        }
    }
}
