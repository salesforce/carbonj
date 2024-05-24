/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DynamoDbCheckPointMgr implements CheckPointMgr<Date> {
    private static final Logger log = LoggerFactory.getLogger(DynamoDbCheckPointMgr.class);

    private final String tableName;
    private final int defaultOffsetMins;

    private final AmazonDynamoDB client;
    private final DynamoDB dynamoDB;

    public DynamoDbCheckPointMgr(AmazonDynamoDB client, String kinesisApplicationName, int defaultOffsetMins,
                                 int provisionedThroughput) throws Exception {
        this.client = client;
        this.dynamoDB = new DynamoDB(client);
        this.defaultOffsetMins = defaultOffsetMins;
        this.tableName = "checkpoints-" + kinesisApplicationName;
        if (!DynamoDbUtils.isTablePresent(dynamoDB, tableName)) {
            createTable(tableName, provisionedThroughput);
        }
    }

    private void createTable(String tableName, int provisionedThroughput) throws Exception {
        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(
                        new AttributeDefinition("checkPointType", ScalarAttributeType.S))
                .withKeySchema(
                        new KeySchemaElement("checkPointType", KeyType.HASH))
                .withProvisionedThroughput(
                        new ProvisionedThroughput((long)provisionedThroughput, (long)provisionedThroughput))
                .withTableName(tableName);
        log.info("Issuing CreateTable request for " + tableName);
        Table newlyCreatedTable = dynamoDB.createTable(request);
        log.info("Waiting for " + tableName + " to be created...this may take a while...");
        newlyCreatedTable.waitForActive();
    }

    @Override
    public void checkPoint(Date checkPoint) throws Exception {
        Table table = dynamoDB.getTable(tableName);

        HashMap<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#V", "checkPointValue");

        HashMap<String, Object> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val1", checkPoint.getTime());

        table.updateItem(
                "checkPointType",          // key attribute name
                "timestamp",           // key attribute value
                "set #V = :val1", // UpdateExpression
                expressionAttributeNames,
                expressionAttributeValues);
    }

    @Override
    public Date lastCheckPoint() throws Exception {
        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
        keyToGet.put( "checkPointType", new AttributeValue( "timestamp") );
        GetItemRequest request = new GetItemRequest()
                .withKey( keyToGet )
                .withTableName( tableName );

        Map<String, AttributeValue> item = client.getItem( request ).getItem();
        if( item == null ) {
            return getDefaultCheckPoint();
        }
        String value = item.get( "checkPointValue" ).getN();
        if( value == null ) {
            return getDefaultCheckPoint();
        }

        return new Date( Long.parseLong( value ) );
    }

    private Date getDefaultCheckPoint() {
        Date checkPoint = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(defaultOffsetMins));
        log.warn("Check point not found!  new checkpoint using default offset: " + checkPoint);
        return checkPoint;
    }
}

