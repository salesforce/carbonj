/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DynamoDbCheckPointMgr implements CheckPointMgr<Date> {
    private static final Logger log = LoggerFactory.getLogger(DynamoDbCheckPointMgr.class);

    private final String tableName;
    private final int defaultOffsetMins;

    private final DynamoDbClient client;

    public DynamoDbCheckPointMgr(DynamoDbClient client, String kinesisApplicationName, int defaultOffsetMins,
                                 int provisionedThroughput) {
        this.client = client;
        this.defaultOffsetMins = defaultOffsetMins;
        this.tableName = "checkpoints-" + kinesisApplicationName;
        if (!DynamoDbUtils.isTablePresent(client, tableName)) {
            createTable(tableName, provisionedThroughput);
        }
    }

    private void createTable(String tableName, int provisionedThroughput) {
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("checkPointType").attributeType(ScalarAttributeType.S).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName("checkPointType").keyType(KeyType.HASH).build())
                .provisionedThroughput(
                        ProvisionedThroughput.builder().readCapacityUnits((long)provisionedThroughput).writeCapacityUnits((long)provisionedThroughput).build())
                .tableName(tableName)
                .build();
        log.info("Issuing CreateTable request for {}", tableName);
        client.createTable(request);
        log.info("Waiting for {} to be created...this may take a while...", tableName);
        try (DynamoDbWaiter waiter = client.waiter()) {
            WaiterResponse<DescribeTableResponse> wr = waiter.waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build());
            wr.matched();
        }
    }

    @Override
    public void checkPoint(Date checkPoint) throws Exception {
        HashMap<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#V", "checkPointValue");

        HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val1", AttributeValue.builder().n(Long.toString(checkPoint.getTime())).build());

        client.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap("checkPointType", AttributeValue.builder().s("timestamp").build()))
                .updateExpression("set #V = :val1")
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build());
    }

    @Override
    public Date lastCheckPoint() {
        HashMap<String, AttributeValue> keyToGet = new HashMap<>();
        keyToGet.put( "checkPointType", AttributeValue.builder().s("timestamp").build() );
        GetItemRequest request = GetItemRequest.builder()
                .key( keyToGet )
                .tableName( tableName )
                .build();

        Map<String, AttributeValue> item = client.getItem( request ).item();
        if( item == null || item.isEmpty() ) {
            return getDefaultCheckPoint();
        }
        String value = item.get( "checkPointValue" ).n();
        if( value == null ) {
            return getDefaultCheckPoint();
        }

        return new Date( Long.parseLong( value ) );
    }

    private Date getDefaultCheckPoint() {
        Date checkPoint = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(defaultOffsetMins));
        log.warn("Check point not found!  new checkpoint using default offset: {}", checkPoint);
        return checkPoint;
    }
}

