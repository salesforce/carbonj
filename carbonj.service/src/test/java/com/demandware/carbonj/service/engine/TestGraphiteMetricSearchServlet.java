/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.model.MsgPackMetric;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.msgpack.jackson.dataformat.MessagePackMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGraphiteMetricSearchServlet extends CarbonJSvcLongIdTest {
    @Test
    public void testGraphiteMetricSearchServlet() throws Exception {
        DateTime now = DateTime.now();
        cjClient.send( "a.b.c", 1.0f, now );
        cjClient.send( "a.b.d", 1.0f, now );
        drain();

        RestTemplate restTemplate = new RestTemplate();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("format", "msgpack");
        queryParams.put("query", "a.b.c");
        URI uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/metrics")
                .queryParam("format", "{format}")
                .queryParam("query", "{query}")
                .build(queryParams);
        HttpEntity<String> httpEntity = new HttpEntity<>(new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        ObjectMapper objectMapper = new MessagePackMapper();
        List<MsgPackMetric> msgPackMetricList = objectMapper.readValue(response.getBody().getBytes(StandardCharsets.ISO_8859_1), new TypeReference<>() {});
        assertEquals(1, msgPackMetricList.size());
        MsgPackMetric msgPackMetric = msgPackMetricList.get(0);
        assertEquals("a.b.c", msgPackMetric.path);
        assertTrue(msgPackMetric.isLeaf);
    }
}
