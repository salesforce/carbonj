package com.demandware.carbonj.service.engine;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.model.MsgPackSeries;

/**
 * ResponseStream implementation that writes MsgPackSeries as binary MessagePack
 * directly to the HttpServletResponse's output stream.
 */
public class MessagePackHttpResponseWriter implements ResponseStream {

    private final OutputStream out;
    private final ObjectMapper objectMapper;

    public MessagePackHttpResponseWriter(HttpServletResponse response) throws IOException {
        this.out = response.getOutputStream();
        this.objectMapper = new ObjectMapper(new MessagePackFactory());
    }

    @Override
    public synchronized void openSeriesList() throws IOException {
        // No-op for MessagePack streaming
    }

    @Override
    public synchronized void writeSeries(Series s) throws IOException {
        MsgPackSeries msgPackSeries = new MsgPackSeries(s);
        byte[] bytes = objectMapper.writeValueAsBytes(msgPackSeries);
        out.write(bytes);
    }

    @Override
    public synchronized void closeSeriesList() throws IOException {
        // No-op for MessagePack streaming
    }

    @Override
    public synchronized void close() throws IOException {
        out.flush();
        // Do not close the servlet response's OutputStream, just flush it.
    }
} 