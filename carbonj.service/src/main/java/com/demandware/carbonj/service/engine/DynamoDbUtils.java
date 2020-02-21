/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDbUtils {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbUtils.class);

    public static boolean isTablePresent(DynamoDB dynamoDB, String tableName) {
        Table table = dynamoDB.getTable(tableName);

        try {
            TableDescription tableDescription = table.describe();
            return "ACTIVE".equals(tableDescription.getTableStatus());
        } catch (ResourceNotFoundException e) {
            log.warn("kinesis consumer table '" + tableName + "' not found!");
            return false;
        }
    }

    public static void deleteTable(DynamoDB dynamoDB, String tableName) throws InterruptedException {
        Table table = dynamoDB.getTable(tableName);
        table.delete();

        log.info("Waiting for " + tableName + " to be deleted...this may take a while...");

        table.waitForDelete();
    }
}
