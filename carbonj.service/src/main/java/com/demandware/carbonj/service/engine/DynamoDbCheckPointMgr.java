/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DynamoDbCheckPointMgr implements CheckPointMgr<Date> {
    private static final Logger log = LoggerFactory.getLogger(DynamoDbCheckPointMgr.class);

    private final String tableName;
    private final int defaultOffsetMins;

    private final DynamoDbAsyncClient client;
    private final int checkPointDynamodbTimout;

    public DynamoDbCheckPointMgr(DynamoDbAsyncClient client, String kinesisApplicationName, int defaultOffsetMins,
                                 int provisionedThroughput, int checkPointDynamodbTimout) throws Exception {
        this.client = client;
        this.defaultOffsetMins = defaultOffsetMins;
        this.tableName = "checkpoints-" + kinesisApplicationName;
        this.checkPointDynamodbTimout = checkPointDynamodbTimout;
        if (!DynamoDbUtils.isTablePresent(client, tableName, checkPointDynamodbTimout)) {
            createTable(tableName, provisionedThroughput);
        }
    }

    private void createTable(String tableName, int provisionedThroughput) throws Exception {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(AttributeDefinition.builder().attributeName("checkPointType").attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName("checkPointType").keyType(KeyType.HASH).build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits((long)provisionedThroughput)
                        .writeCapacityUnits((long)provisionedThroughput)
                        .build())
                .build();
        log.info("Issuing CreateTable request for " + tableName);
        CompletableFuture<CreateTableResponse> createTableResponse = this.client.createTable(request);
        log.info("Waiting for " + tableName + " to be created...this may take a while...");
        createTableResponse.get(checkPointDynamodbTimout, TimeUnit.SECONDS);
    }

    @Override
    public void checkPoint(Date checkPoint) throws Exception {

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#V", "checkPointValue");

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val1", AttributeValue.builder().n(String.valueOf(checkPoint.getTime())).build());

        client.updateItem(UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(Map.of("checkPointType", AttributeValue.builder().s("timestamp").build()))
                        .updateExpression("set #V = :val1")
                        .expressionAttributeNames(expressionAttributeNames)
                        .expressionAttributeValues(expressionAttributeValues).build());
    }

    @Override
    public Date lastCheckPoint() throws Exception {

        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("checkPointType", AttributeValue.builder().s("timestamp").build()))
                .build();

        GetItemResponse getItemResponse = this.client.getItem(request).get(checkPointDynamodbTimout, TimeUnit.SECONDS);

        if (!getItemResponse.hasItem()) {
            return getDefaultCheckPoint();
        }

        Map<String, AttributeValue> item = getItemResponse.item();
        String value = item.get("checkPointValue").n();
        if( value == null ) {
            return getDefaultCheckPoint();
        }

        return new Date(Long.parseLong(value));
    }

    private Date getDefaultCheckPoint() {
        Date checkPoint = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(defaultOffsetMins));
        log.warn("Check point not found!  new checkpoint using default offset: " + checkPoint);
        return checkPoint;
    }
}
