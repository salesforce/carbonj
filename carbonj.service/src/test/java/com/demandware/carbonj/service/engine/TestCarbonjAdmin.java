/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.demandware.carbonj.service.db.model.RetentionPolicy;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestCarbonjAdmin extends CarbonJSvcLongIdTest {
    @Test
    public void testCarbonjAdmin() {
        cjClient.send( "a.b.c", 1.0f, new DateTime() );
        cjClient.send( "a.b.d", 1.0f, new DateTime() );
        drain();

        RestTemplate restTemplate = new RestTemplate();
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("srcIp", "127.0.0.1");
        queryParams.put("srcPort", "2001");
        URI uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/loadseriesfrom/60s24h")
                .queryParam("srcIp", "{srcIp}")
                .queryParam("srcPort", "{srcPort}")
                .build(queryParams);
        HttpEntity<String> httpEntity = new HttpEntity<>(new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertEquals("a.b.c:2 1 totalPoints:1 cursor:0 totalMetrics:1\n" +
                "a.b.d:3 1 totalPoints:2 cursor:0 totalMetrics:2\n", response.getBody());

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/abortload").build().toUri();
        response = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertEquals("load is not running", response.getBody());

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/loadseries/60s24h").build().toUri();
        response = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertNull(response.getBody());

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/metrics/a.b.c").build().toUri();
        response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertTrue(response.getBody().startsWith("{\"id\":2,\"name\":\"a.b.c\""));

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/metrics/foo.bar").build().toUri();
        try {
            restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"Metric [foo.bar] not found.\"}\"", e.getMessage());
        }

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/metricsearch").build().toUri();
        try {
            restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"request parameter 'metricId' should be a positive int, Actual value [0]\"}\"", e.getMessage());
        }

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/metricsearch?metricId=1234").build().toUri();
        try {
            restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"Metric with id [1234] not found.\"}\"", e.getMessage());
        }

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/metricsearch?metricId=2").build().toUri();
        response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertTrue(response.getBody().startsWith("[{\"id\":2,\"name\":\"a.b.c\""));

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/getIdStoreName/a.b.c").build().toUri();
        response = restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertEquals("a.b.c", response.getBody());

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/getIdStoreName/foo.bar").build().toUri();
        try {
            restTemplate.exchange(uri, HttpMethod.GET, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"Metric [foo.bar] not found.\"}\"", e.getMessage());
        }

        int current = (int) (System.currentTimeMillis() / 1000);
        queryParams.put("points", "" + current);
        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/dbloader/60s7d/foo.bar")
                .queryParam("points", "{points}")
                .build(queryParams);
        try {
            restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"Unknown database [60s7d]\"}\"", e.getMessage());
        }

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/dbloader/60s24h/foo.bar")
                .queryParam("points", "{points}")
                .build(queryParams);
        try {
            restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"Metric not found. Metric name: [foo.bar]\"}\"", e.getMessage());
        }

        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/dbloader/60s24h/a.b.c")
                .queryParam("points", "{points}")
                .build(queryParams);
        try {
            restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
            fail("Should have thrown exception");
        } catch (HttpServerErrorException e) {
            assertEquals("500 Server Error: \"{\"status\":\"ERROR\",\"message\":\"Invalid import line format: [" + current + "]\"}\"", e.getMessage());
        }

        RetentionPolicy retentionPolicy = RetentionPolicy.getInstance("60s:24h");
        queryParams.put("points", retentionPolicy.interval(current) + ",123.45");
        queryParams.put("maxImportErrors", "1");
        uri = UriComponentsBuilder.fromHttpUrl("http://127.0.0.1:2001/_dw/rest/carbonj/dbloader/60s24h/a.b.c")
                .queryParam("points", "{points}")
                .queryParam("maxImportErrors", "{maxImportErrors}")
                .build(queryParams);
        response = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, String.class);
        assertEquals(HttpStatus.OK.value(), response.getStatusCode().value());
        assertEquals("{\"dbName\":\"60s24h\",\"received\":1,\"saved\":1,\"errors\":0,\"expired\":0}", response.getBody());
    }
}
