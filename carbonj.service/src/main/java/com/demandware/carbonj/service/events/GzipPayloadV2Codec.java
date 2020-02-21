/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.events;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Payload version 2 encoding and decoding is handled by this class.  Basically,  it compresses the input.
 */
public class GzipPayloadV2Codec implements PayloadV2Codec {

    private static final GzipPayloadV2Codec INSTANCE = new GzipPayloadV2Codec();

    private GzipPayloadV2Codec() {}

    static GzipPayloadV2Codec getInstance() {
        return INSTANCE;
    }

    @Override
    public byte[] encode(Collection<byte[]> eventCollections) throws Exception {
        // calculate the size upfront so that we have less array copy
        Iterator<byte[]> eventCollectionsIterator = eventCollections.iterator();
        int size = 0;
        while (eventCollectionsIterator.hasNext()) {
            size += eventCollectionsIterator.next().length;
        }

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream(size);
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut); 
             DataOutputStream dos = new DataOutputStream(gzipOut)) {
            dos.writeInt(eventCollections.size());
            for (byte[] eventCollection : eventCollections) {
                //Write length
                dos.writeInt(eventCollection.length);
                // write the data bytes
                dos.write(eventCollection);
            }
            dos.flush();
        }
        return byteOut.toByteArray();
    }

    @Override
    public Collection<byte[]> decode(byte[] encodedBytes) throws Exception {
        List<byte[]> batch = new ArrayList<>();
        try (GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(encodedBytes));
             DataInputStream dis = new DataInputStream(new BufferedInputStream(gzis))) {

            int eventCollectionsSize = dis.readInt();
            for (int i = 0; i < eventCollectionsSize; i++) {
                int length = dis.readInt();
                byte[] eventCollection = new byte[length];
                //noinspection ResultOfMethodCallIgnored
                dis.read(eventCollection);

                batch.add(eventCollection);
            }
        }

        return batch;
    }
}
