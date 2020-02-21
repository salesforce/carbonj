/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine.recovery;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.demandware.carbonj.service.engine.DynamoDbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DynamoDbGapsTableImpl implements GapsTable {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbGapsTableImpl.class);

    private final DynamoDB dynamoDB;
    private final String tableName;
    private final AmazonDynamoDB client;

    public DynamoDbGapsTableImpl(AmazonDynamoDB client, String kinesisApplicationName, int gapsTableProvThroughput ) throws Exception {

        this.dynamoDB = new DynamoDB(client);
        this.tableName = "gaps-" + kinesisApplicationName;
        this.client = client;

        if (!DynamoDbUtils.isTablePresent(dynamoDB, tableName)) {
            createTable(tableName, gapsTableProvThroughput);
        }
    }

    private void createTable(String tableName, int provisionedThroughput) throws Exception {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition("starttime", ScalarAttributeType.N));

        List<KeySchemaElement> ks = new ArrayList<>();
        ks.add(new KeySchemaElement("starttime", KeyType.HASH));

        ProvisionedThroughput provisionedthroughput =
                new ProvisionedThroughput(new Long(provisionedThroughput), new Long(provisionedThroughput));

        CreateTableRequest request =
                new CreateTableRequest()
                        .withTableName(tableName)
                        .withAttributeDefinitions(attributeDefinitions)
                        .withKeySchema(ks)
                        .withProvisionedThroughput(provisionedthroughput);

        log.info("Issuing CreateTable request for " + tableName);
        Table newlyCreatedTable = dynamoDB.createTable(request);

        log.info("Waiting for " + tableName + " to be created...this may take a while...");
        newlyCreatedTable.waitForActive();
    }

    @Override
    public void add(Gap gap) {
        Table table = dynamoDB.getTable(tableName);

        table.putItem(new Item().withPrimaryKey("starttime", gap.startTime().getTime())
                .withLong("endtime", gap.endTime().getTime())
                .withLong("lastRecovered", gap.lastRecovered().getTime()));

        log.info("Added gap: " + gap);
    }

    @Override
    public List<Gap> getGaps() {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName);

        List<Gap> gaps = new ArrayList<>();

        ScanResult result = client.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()){
            Date starttime = new Date(Long.parseLong(item.get("starttime").getN()));
            Date endtime = new Date(Long.parseLong(item.get("endtime").getN()));
            Date lastRecovered = new Date(Long.parseLong(item.get("lastRecovered").getN()));
            gaps.add(new GapImpl(starttime, endtime, lastRecovered));
        }

        Collections.sort(gaps);
        return gaps;
    }

    @Override
    public void delete(Gap gap) {
        Table table = dynamoDB.getTable(tableName);

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(new PrimaryKey("starttime", gap.startTime().getTime()));

        table.deleteItem(deleteItemSpec);
        log.info("Gap deleted: " + gap);
    }

    @Override
    public boolean updateGap(Gap gap) {
        Table table = dynamoDB.getTable(tableName);

        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#E", "endtime");
        expressionAttributeNames.put("#L", "lastRecovered");

        Map<String, Object> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val1", gap.endTime().getTime());
        expressionAttributeValues.put(":val2", gap.lastRecovered().getTime());

        table.updateItem(
                "starttime",          // key attribute name
                gap.startTime().getTime(),           // key attribute value
                "set #E = :val1, #L = :val2", // UpdateExpression
                expressionAttributeNames,
                expressionAttributeValues);

        return true;
    }

    @Override
    public void destroy() {
        try {
            DynamoDbUtils.deleteTable(dynamoDB, tableName);
        } catch (InterruptedException e) {
            log.error("Interrupted while destroting " + tableName, e);
        }
    }
}
