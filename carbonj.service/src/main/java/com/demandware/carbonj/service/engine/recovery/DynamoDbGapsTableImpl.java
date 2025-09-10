/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import com.demandware.carbonj.service.engine.DynamoDbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDbGapsTableImpl implements GapsTable {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbGapsTableImpl.class);

    private final DynamoDbClient dynamoDB;
    private final String tableName;

    public DynamoDbGapsTableImpl(DynamoDbClient client, String kinesisApplicationName, int gapsTableProvThroughput ) {

        this.dynamoDB = client;
        this.tableName = "gaps-" + kinesisApplicationName;

        if (!DynamoDbUtils.isTablePresent(dynamoDB, tableName)) {
            createTable(tableName, gapsTableProvThroughput);
        }
    }

    private void createTable(String tableName, int provisionedThroughput) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName("starttime").attributeType(ScalarAttributeType.N).build());

        List<KeySchemaElement> ks = new ArrayList<>();
        ks.add(KeySchemaElement.builder().attributeName("starttime").keyType(KeyType.HASH).build());

        ProvisionedThroughput provisionedthroughput =
                ProvisionedThroughput.builder().readCapacityUnits((long)provisionedThroughput).writeCapacityUnits((long)provisionedThroughput).build();

        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(attributeDefinitions)
                .keySchema(ks)
                .provisionedThroughput(provisionedthroughput)
                .build();

        log.info("Issuing CreateTable request for {}", tableName);
        dynamoDB.createTable(request);

        log.info("Waiting for {} to be created...this may take a while...", tableName);
        try (DynamoDbWaiter waiter = dynamoDB.waiter()) {
            WaiterResponse<?> waiterResponse = waiter.waitUntilTableExists(b -> b.tableName(tableName));
            waiterResponse.matched();
        }
    }

    @Override
    public void add(Gap gap) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("starttime", AttributeValue.builder().n(Long.toString(gap.startTime().getTime())).build());
        item.put("endtime", AttributeValue.builder().n(Long.toString(gap.endTime().getTime())).build());
        item.put("lastRecovered", AttributeValue.builder().n(Long.toString(gap.lastRecovered().getTime())).build());

        dynamoDB.putItem(b -> b.tableName(tableName).item(item));

        log.info("Added gap: {}", gap);
    }

    @Override
    public List<Gap> getGaps() {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();

        List<Gap> gaps = new ArrayList<>();

        ScanResponse result = dynamoDB.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.items()){
            Date starttime = new Date(Long.parseLong(item.get("starttime").n()));
            Date endtime = new Date(Long.parseLong(item.get("endtime").n()));
            Date lastRecovered = new Date(Long.parseLong(item.get("lastRecovered").n()));
            gaps.add(new GapImpl(starttime, endtime, lastRecovered));
        }

        Collections.sort(gaps);
        return gaps;
    }

    @Override
    public void delete(Gap gap) {
        dynamoDB.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap("starttime", AttributeValue.builder().n(Long.toString(gap.startTime().getTime())).build()))
                .build());
        log.info("Gap deleted: {}", gap);
    }

    @Override
    public boolean updateGap(Gap gap) {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#E", "endtime");
        expressionAttributeNames.put("#L", "lastRecovered");

        Map<String, AttributeValue> key = Collections.singletonMap("starttime",
                AttributeValue.builder().n(Long.toString(gap.startTime().getTime())).build());

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val1", AttributeValue.builder().n(Long.toString(gap.endTime().getTime())).build());
        expressionAttributeValues.put(":val2", AttributeValue.builder().n(Long.toString(gap.lastRecovered().getTime())).build());

        dynamoDB.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("set #E = :val1, #L = :val2")
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build());

        return true;
    }

    @Override
    public void destroy() {
        try {
            DynamoDbUtils.deleteTable(dynamoDB, tableName);
        } catch (InterruptedException e) {
            log.error("Interrupted while destroting {}", tableName, e);
        }
    }
}
