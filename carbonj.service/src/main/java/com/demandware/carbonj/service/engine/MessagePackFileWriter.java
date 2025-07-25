/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import java.io.*;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.model.MsgPackSeries;

public class MessagePackFileWriter implements ResponseStream {
    private final File tempFile;
    private OutputStream outputStream;
    private final ObjectMapper objectMapper;
    private boolean isOpen = false;
    private final HttpServletResponse response;

    public MessagePackFileWriter(String carbonjRenderDir, HttpServletResponse response) throws IOException {
        File renderTmpDir = new File(carbonjRenderDir).getAbsoluteFile();
        if (!renderTmpDir.exists()) {
            if (!renderTmpDir.mkdirs() && !renderTmpDir.exists()) {
                throw new IOException("Failed to create render-tmp directory: " + renderTmpDir.getAbsolutePath());
            }
        }
        // Create a unique temp file in render-tmp
        String unique = Thread.currentThread().getId() + "_" + System.nanoTime() + "_" + UUID.randomUUID();
        this.tempFile = new File(renderTmpDir, "msgpack_series_" + unique + ".bin");
        this.objectMapper = new ObjectMapper(new MessagePackFactory());
        this.response = response;
    }

    public File getTempFile() {
        return tempFile;
    }

    @Override
    public synchronized void openSeriesList() throws IOException {
        if (isOpen) return;
        this.outputStream = new FileOutputStream(tempFile);
        isOpen = true;
    }

    @Override
    public synchronized void writeSeries(Series s) throws IOException {
        if (!isOpen) throw new IOException("Stream not open");
        MsgPackSeries msgPackSeries = new MsgPackSeries(s);
        byte[] bytes = objectMapper.writeValueAsBytes(msgPackSeries);
        outputStream.write(bytes);
    }

    @Override
    public synchronized void closeSeriesList() {
        // No-op for this streaming approach
    }

    @Override
    public synchronized void close() throws IOException {
        if (outputStream != null) {
            // Now stream the file to the HTTP response
            response.setContentLengthLong(tempFile.length());
            try (OutputStream out = response.getOutputStream();
                 FileInputStream in = new FileInputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
            // Close the temp file
            outputStream.close();
            outputStream = null;
            isOpen = false;
        }
    }
}
