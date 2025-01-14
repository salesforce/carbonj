/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.AssertionErrors.assertEquals;
import static org.springframework.test.util.AssertionErrors.assertTrue;

public class _ConfigServerUtil {

    private final String backupFile = "work/config-server-bkup.txt";

    private final Path backupFilePath = Paths.get(backupFile);

    private final String configServerBaseUrl = "http://localhost:8080";

    private final String registrationUrl = String.format("%s/rest/v1/processes/register", configServerBaseUrl);

    private final String processUniqueId = "pod1";

    private final String processHost = "util1";

    private final String uniqueId = String.format("cc1p.cjrelay.%s", processUniqueId);

    private final ConfigServerUtil.ProcessConfig idBasedRelayRules = new ConfigServerUtil.ProcessConfig(1L,
            "id-based-relay-rules", "cc1p.cjrelay", "^pod1\\..*=kinesisS1\n",
            1, "Init commit", "unknown", new Date());

    private final ConfigServerUtil.ProcessConfig relayRules = new ConfigServerUtil.ProcessConfig(2L, "relay-rules",
            "cc1p.cjrelay", "^pi\\..*=kinesisS1\n\n", 1, "Init commit", "unknown", new Date());

    private final ConfigServerUtil.Process process = new ConfigServerUtil.Process(1L, uniqueId, processHost, new Date(),
            new Date(), Arrays.asList(idBasedRelayRules, relayRules));

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Counter successCounter;

    private Counter failureCounter;

    private Counter configLookupFailureCounter;

    private MetricRegistry metricRegistry;

    @BeforeEach
    public void setup() throws IOException
    {
        if ( Files.exists( backupFilePath ) )
        {
            Files.delete( backupFilePath );
        }
        if ( !Files.exists( backupFilePath.getParent() ) )
        {
            Files.createDirectory( backupFilePath.getParent() );
        }
        // Mock metric registry
        successCounter = mock(Counter.class);
        failureCounter = mock(Counter.class);
        configLookupFailureCounter = mock(Counter.class);
        metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.counter(anyString())).thenAnswer((Answer<Counter>) invocation -> {
            String arg = invocation.getArgument(0);
            if (arg.contains("registration.failed")) {
                return failureCounter;
            } else if (arg.contains("registration.success")) {
                return successCounter;
            } else if (arg.contains("lookup.failure")) {
                return configLookupFailureCounter;
            } else {
                return null;
            }
        });
    }

    @Test
    public void testRegister_success() throws IOException {
        // Mock rest template
        RestTemplate restTemplate = mock(RestTemplate.class);
        when(restTemplate.postForEntity(eq(registrationUrl), notNull(), notNull()))
                .thenReturn(new ResponseEntity<>(process, HttpStatus.OK));
        ConfigServerUtil configServerUtil = new ConfigServerUtil(restTemplate, configServerBaseUrl, metricRegistry,
                processUniqueId, backupFile);
        assertEquals("Merge failed. Invalid config.", Arrays.asList("^pod1\\..*=kinesisS1", "^pi\\..*=kinesisS1"),
                configServerUtil.getConfigLines("relay-rules").get());
        assertTrue("Backup file does not exist: " + backupFilePath, Files.exists(backupFilePath));
        assertEquals("Invalid backup file content", process.getProcessConfigs().get(0).getValue(),
                objectMapper.readValue(Files.readAllBytes(backupFilePath),
                        ConfigServerUtil.Process.class).getProcessConfigs().get(0).getValue());
        verify(successCounter, times(1)).inc(0);
        verify(successCounter, times(1)).inc();
        verify(failureCounter, times(1)).inc(0);
        verify(configLookupFailureCounter, times(1)).inc(0);
    }

    @Test
    public void testRegister_failure() throws IOException {
        // Create backup file
        if (!Files.exists(backupFilePath)) {
            Files.createFile(backupFilePath);
        }
        try (BufferedWriter writer = Files.newBufferedWriter(backupFilePath)) {
            writer.write("");
            writer.flush();
        }
        objectMapper.writeValue(backupFilePath.toFile(), process);
        // Mock rest template
        RestTemplate restTemplate = mock(RestTemplate.class);
        when(restTemplate.postForEntity(eq(registrationUrl), notNull(), notNull()))
                .thenReturn(new ResponseEntity<>("Failed", HttpStatus.INTERNAL_SERVER_ERROR));
        ConfigServerUtil configServerUtil = new ConfigServerUtil(restTemplate, configServerBaseUrl, metricRegistry,
                processUniqueId, backupFile);
        assertTrue("Backup file does not exist", Files.exists(backupFilePath));
        assertEquals("Invalid config", Arrays.asList("^pod1\\..*=kinesisS1", "^pi\\..*=kinesisS1"),
                configServerUtil.getConfigLines("relay-rules").get());
        assertEquals("Invalid backup file content", process.getProcessConfigs().get(0).getValue(),
                objectMapper.readValue(Files.readAllBytes(backupFilePath),
                        ConfigServerUtil.Process.class).getProcessConfigs().get(0).getValue());
        verify(successCounter, times(1)).inc(0);
        verify(failureCounter, times(1)).inc(0);
        verify(failureCounter, times(1)).inc();
        verify(configLookupFailureCounter, times(1)).inc(0);
    }

    @Test
    public void testOthers() {
        ConfigServerUtil.ProcessConfig processConfig = new ConfigServerUtil.ProcessConfig();
        assertTrue("Same object", processConfig.equals(processConfig));
        assertFalse(processConfig.equals(new Object()));
        assertTrue("Same object", processConfig.equals(new ConfigServerUtil.ProcessConfig()));
        assertNotEquals(0, processConfig.hashCode());
        assertEquals("The same toString", "ProcessConfig{id=0, name='null', processId='null', value='null', version=0, message='null', author='null', lastModifiedDate=null}", processConfig.toString());

        ConfigServerUtil.Process process = new ConfigServerUtil.Process();
        assertTrue("Same object", process.equals(process));
        assertFalse(process.equals(new Object()));
        assertTrue("Same object", process.equals(new ConfigServerUtil.Process()));
        assertNotEquals(0, process.hashCode());
        assertEquals("The same toString", "Process{id=0, uniqueId='null', host='null', firstRegistrationDate=null, lastRegistrationDate=null, processConfigs=null}", process.toString());
    }
}
