/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ConfigServerUtil {

    private static final Logger log = LoggerFactory.getLogger(ConfigServerUtil.class);

    private volatile Map<String, ProcessConfig> nameToConfig;

    private final RestTemplate restTemplate;

    private final String registrationUrl;

    private final Counter registrationSuccessCount;

    private final Counter registrationFailureCount;

    private final Counter configLookupFailureCount;

    private final String processUniqueId;

    private final String processHost;

    private final Path backupFilePath;

    private final ObjectMapper objectMapper;

    public ConfigServerUtil(RestTemplate restTemplate, String configServerBaseUrl, MetricRegistry metricRegistry,
                            String processUniqueId, String backupFilePath) throws IOException {
        this.restTemplate = restTemplate;
        this.registrationUrl = String.format("%s/rest/v1/processes/register", configServerBaseUrl);
        this.registrationFailureCount = metricRegistry.counter(MetricRegistry.name("configServer", "registration",
                "failed"));
        this.registrationFailureCount.inc(0);
        this.registrationSuccessCount = metricRegistry.counter(MetricRegistry.name("configServer", "registration",
                "success"));
        this.registrationSuccessCount.inc(0);
        this.configLookupFailureCount = metricRegistry.counter(MetricRegistry.name("configServer", "lookup",
                "failure"));
        this.configLookupFailureCount.inc(0);
        this.processUniqueId = processUniqueId;
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Unable to determine host name.", e);
            hostName = "unknown";
        }
        this.processHost = hostName;
        this.nameToConfig = new ConcurrentHashMap<>();
        this.backupFilePath = Paths.get(backupFilePath);
        this.objectMapper = new ObjectMapper();
        // For backward compatibility
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        log.info("Config server registry initialised. Registration URL {}", registrationUrl);
        register();
    }

    public Optional<List<String>> getConfigLines(String name) {
        if (nameToConfig.containsKey(name)) {
            return Optional.of(Arrays.stream(nameToConfig.get(name).getValue().split("\n"))
                    .filter(l -> !l.isEmpty())
                    .collect(Collectors.toList()));
        } else {
            this.configLookupFailureCount.inc();
            return Optional.empty();
        }
    }

    public synchronized void register() throws IOException {
        try {
            final ResponseEntity<Process> res = restTemplate.postForEntity(registrationUrl, getRegistrationRequest(),
                    Process.class);
            if (res.getStatusCode().value() >= 200 && res.getStatusCode().value() < 300) {
                Process process = res.getBody();
                if (process != null && process.getProcessConfigs() != null && !process.getProcessConfigs().isEmpty()) {
                    updateConfig(process);
                    saveToBackupFile(process);
                    registrationSuccessCount.inc();
                    log.info("Registered successfully with config server at {}, received configs: {}", registrationUrl,
                            process.getProcessConfigs().stream().map(ProcessConfig::getName).collect(Collectors.toList()));
                } else {
                    handleRegistrationFailure(String.format("Invalid response from registration server. Process: %s",
                            process), null);
                }
            } else {
                handleRegistrationFailure(String.format("Config server registration failed. URL: %s, Response status " +
                        "code: %s", registrationUrl, res.getStatusCodeValue()), null);
            }
        } catch (Exception e) {
            handleRegistrationFailure("Unexpected error during config server registration. URL: " + registrationUrl, e);
        }
    }

    private void saveToBackupFile(Process process) throws IOException {
        if (!Files.exists(backupFilePath)) {
            Files.createFile(backupFilePath);
        }
        // Clear content
        BufferedWriter writer = Files.newBufferedWriter(backupFilePath);
        writer.write("");
        writer.flush();
        objectMapper.writeValue(backupFilePath.toFile(), process);
    }

    private void updateConfig(Process process) {
        final List<ProcessConfig> idBasedConfigs = process.getProcessConfigs().stream()
                .filter(pc -> pc.getName().startsWith("id-based"))
                .collect(Collectors.toList());
        final Map<String, ProcessConfig> nameToConfigTmp = process.getProcessConfigs().stream()
                .filter(pc -> !pc.getName().startsWith("id-based"))
                .collect(Collectors.toMap(ProcessConfig::getName, pc -> pc));
        // Merge id based configs
        idBasedConfigs.forEach(pc -> {
            final String genericConfigName = pc.getName().replaceFirst("id-based-", "");
            if (nameToConfigTmp.containsKey(genericConfigName)) {
                ProcessConfig genericConfig = nameToConfigTmp.get(genericConfigName);
                if (pc.getValue().endsWith("\n")) {
                    genericConfig.setValue(pc.getValue() + genericConfig.getValue());
                } else {
                    genericConfig.setValue(pc.getValue() + "\n" + genericConfig.getValue());
                }
            } else {
                log.warn("Config merge failed. Generic config not found for id based config {}", pc.getName());
            }
        });
        this.nameToConfig.putAll(nameToConfigTmp);
    }

    private void handleRegistrationFailure(String errMessage, Exception e) throws IOException {
        if (e != null) {
            log.error(errMessage, e);
        } else {
            log.error(errMessage);
        }
        registrationFailureCount.inc();
        // Try reading previously pulled configuration from backup file.
        if (Files.exists(backupFilePath) && Files.isReadable(backupFilePath) && Files.size(backupFilePath) > 0) {
            Process process = objectMapper.readValue(Files.readAllBytes(backupFilePath), Process.class);
            log.error("Updating config from backup file: {}", backupFilePath);
            updateConfig(process);
        } else {
            log.error("Unable to read configuration from back up file: {}", backupFilePath);
        }
    }

    private Process getRegistrationRequest() {
        return new Process(processUniqueId, processHost);
    }

    static class ProcessConfig {

        private long id;

        private String name;

        private String processId;

        private String value;

        private int version;

        private String message;

        private String author;

        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "MM/dd/yyyy HH:mm:ss")
        private Date lastModifiedDate;

        public ProcessConfig() {
        }

        public ProcessConfig(long id, String name, String processId, String value, int version, String message,
                             String author, Date lastModifiedDate) {
            this.id = id;
            this.name = name;
            this.processId = processId;
            this.value = value;
            this.version = version;
            this.message = message;
            this.author = author;
            this.lastModifiedDate = lastModifiedDate;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getProcessId() {
            return processId;
        }

        public void setProcessId(String processId) {
            this.processId = processId;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public Date getLastModifiedDate() {
            return lastModifiedDate;
        }

        public void setLastModifiedDate(Date lastModifiedDate) {
            this.lastModifiedDate = lastModifiedDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProcessConfig that = (ProcessConfig) o;
            return id == that.id &&
                    version == that.version &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(processId, that.processId) &&
                    Objects.equals(value, that.value) &&
                    Objects.equals(message, that.message) &&
                    Objects.equals(author, that.author) &&
                    Objects.equals(lastModifiedDate, that.lastModifiedDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, processId, value, version, message, author, lastModifiedDate);
        }

        @Override
        public String toString() {
            return "ProcessConfig{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", processId='" + processId + '\'' +
                    ", value='" + value + '\'' +
                    ", version=" + version +
                    ", message='" + message + '\'' +
                    ", author='" + author + '\'' +
                    ", lastModifiedDate=" + lastModifiedDate +
                    '}';
        }
    }

    static class Process {

        private long id;

        private String uniqueId;

        private String host;

        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "MM/dd/yyyy HH:mm:ss")
        private Date firstRegistrationDate;

        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "MM/dd/yyyy HH:mm:ss")
        private Date lastRegistrationDate;

        private List<ProcessConfig> processConfigs;

        public Process() {
        }

        public Process(long id, String uniqueId, String host, Date firstRegistrationDate, Date lastRegistrationDate,
                       List<ProcessConfig> processConfigs) {
            this.id = id;
            this.uniqueId = uniqueId;
            this.host = host;
            this.firstRegistrationDate = firstRegistrationDate;
            this.lastRegistrationDate = lastRegistrationDate;
            this.processConfigs = processConfigs;
        }

        public Process(String uniqueId, String host) {
            this.uniqueId = uniqueId;
            this.host = host;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getUniqueId() {
            return uniqueId;
        }

        public void setUniqueId(String uniqueId) {
            this.uniqueId = uniqueId;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public Date getFirstRegistrationDate() {
            return firstRegistrationDate;
        }

        public void setFirstRegistrationDate(Date firstRegistrationDate) {
            this.firstRegistrationDate = firstRegistrationDate;
        }

        public Date getLastRegistrationDate() {
            return lastRegistrationDate;
        }

        public void setLastRegistrationDate(Date lastRegistrationDate) {
            this.lastRegistrationDate = lastRegistrationDate;
        }

        public List<ProcessConfig> getProcessConfigs() {
            return processConfigs;
        }

        public void setProcessConfigs(List<ProcessConfig> processConfigs) {
            this.processConfigs = processConfigs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Process process = (Process) o;
            return id == process.id &&
                    Objects.equals(uniqueId, process.uniqueId) &&
                    Objects.equals(host, process.host) &&
                    Objects.equals(firstRegistrationDate, process.firstRegistrationDate) &&
                    Objects.equals(lastRegistrationDate, process.lastRegistrationDate) &&
                    Objects.equals(processConfigs, process.processConfigs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, uniqueId, host, firstRegistrationDate, lastRegistrationDate, processConfigs);
        }

        @Override
        public String toString() {
            return "Process{" +
                    "id=" + id +
                    ", uniqueId='" + uniqueId + '\'' +
                    ", host='" + host + '\'' +
                    ", firstRegistrationDate=" + firstRegistrationDate +
                    ", lastRegistrationDate=" + lastRegistrationDate +
                    ", processConfigs=" + processConfigs +
                    '}';
        }
    }
}
