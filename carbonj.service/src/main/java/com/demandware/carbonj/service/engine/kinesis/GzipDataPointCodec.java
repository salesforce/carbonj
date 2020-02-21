/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.kinesis;

import com.demandware.carbonj.service.engine.DataPoint;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipDataPointCodec implements DataPointCodec {

    private static final Logger log = LoggerFactory.getLogger(GzipDataPointCodec.class);

    @Override
    public DataPoints decode(byte[] message) {
        long timeStamp = 0;
        List<DataPoint> batch = new ArrayList<>();
        try (
                ByteArrayInputStream byteIn = new ByteArrayInputStream(message);
                GZIPInputStream gis = new GZIPInputStream(byteIn);
                BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"))) {

            /* First line is the time stamp the message was produced*/
            String line = br.readLine();
            timeStamp = Long.parseLong(line);

            while((line = br.readLine()) != null) {
                String[] parts = StringUtils.split(line, ' ');
                if (parts.length != 3) {
                    log.error("Bad format:" + line);
                } else {
                    batch.add(new DataPoint(parts[0], Double.parseDouble(parts[1]), Integer.parseInt(parts[2])));
                }
            }
        } catch (IOException e) {
            log.error("Error while deserializing the message " + e.getMessage(), e);
        }
        return new DataPoints(batch, timeStamp);
    }

    @Override
    public byte[] encode(DataPoints dataPoints) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut)) {

            /*first line is reserved for message time.*/
            Long time = dataPoints.getTimeStamp();
            gzipOut.write((time.toString() + "\n").getBytes());
            for (DataPoint p : dataPoints.getDataPoints()) {
                // appending new line operator to identify different data points
                String dataPoint = p.toString() + "\n";
                gzipOut.write(dataPoint.getBytes());
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return byteOut.toByteArray();
    }
}
