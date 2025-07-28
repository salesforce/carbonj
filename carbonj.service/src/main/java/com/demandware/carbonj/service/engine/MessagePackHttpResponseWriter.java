package com.demandware.carbonj.service.engine;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import com.demandware.carbonj.service.db.model.Series;
import com.demandware.carbonj.service.db.model.MsgPackSeries;

/**
 * ResponseStream implementation that writes MsgPackSeries as binary MessagePack
 * directly to the HttpServletResponse's output stream.
 */
public class MessagePackHttpResponseWriter implements ResponseStream {

    private final OutputStream out;
    private final ObjectMapper objectMapper;
    private JsonGenerator generator;

    public MessagePackHttpResponseWriter(HttpServletResponse response) throws IOException {
        this.out = response.getOutputStream();
        this.objectMapper = new ObjectMapper(new MessagePackFactory());
        this.generator = null;
    }

    @Override
    public synchronized void openSeriesList() throws IOException {
        // Initialize the generator and start the array
        this.generator = objectMapper.getFactory().createGenerator(out);
        generator.writeStartArray();
    }

    @Override
    public synchronized void writeSeries(Series s) throws IOException {
        MsgPackSeries msgPackSeries = new MsgPackSeries(s);
        objectMapper.writeValue(generator, msgPackSeries);
    }

    @Override
    public synchronized void closeSeriesList() throws IOException {
        // End the array and flush the generator
        if (generator != null) {
            generator.writeEndArray();
            generator.flush();
            generator = null;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        out.flush();
        // Do not close the servlet response's OutputStream, just flush it.
    }
} 