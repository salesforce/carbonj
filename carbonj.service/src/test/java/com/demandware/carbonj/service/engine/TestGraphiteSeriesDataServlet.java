/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.model.MsgPackSeries;
import com.fasterxml.jackson.core.TreeNode;
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

public class TestGraphiteSeriesDataServlet extends CarbonJSvcLongIdTest {
    @Test
    public void test() throws Exception {
        DateTime now = DateTime.now();
        cjClient.send( "a.b.c", 1.0f, now );
        cjClient.send( "a.b.d", 1.0f, now );
        drain();

        RestTemplate restTemplate = new RestTemplate();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("format", "json");
        queryParams.put("target", "a.b.c");
        queryParams.put("from", String.valueOf(now.getMillis() / 1000 - 60));
        queryParams.put("until", String.valueOf(now.getMillis() / 1000 + 60));
        URI uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/render")
                .queryParam("format", "{format}")
                .queryParam("target", "{target}")
                .queryParam("from", "{from}")
                .queryParam("until", "{until}")
                .build(queryParams);
        HttpEntity<String> httpEntity = new HttpEntity<>(new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        ObjectMapper objectMapper = new ObjectMapper();
        TreeNode root = objectMapper.readTree(response.getBody());
        assertTrue(root.isArray());
        assertEquals(1, root.size());
        assertEquals("\"a.b.c\"", root.get(0).get("name").toString());

        queryParams.put("format", "msgpack");
        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/render")
                .queryParam("format", "{format}")
                .queryParam("target", "{target}")
                .queryParam("from", "{from}")
                .queryParam("until", "{until}")
                .build(queryParams);
        httpEntity = new HttpEntity<>(new HttpHeaders());
        response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        ObjectMapper objectMapper2 = new MessagePackMapper();
        List<MsgPackSeries> msgPackSeriesList = objectMapper2.readValue(response.getBody().getBytes(StandardCharsets.ISO_8859_1), new TypeReference<>() {});
        assertEquals(1, msgPackSeriesList.size());
        MsgPackSeries msgPackSeries = msgPackSeriesList.get(0);
        assertEquals("a.b.c", msgPackSeries.name);
        assertEquals(60, msgPackSeries.step);
    }
}
