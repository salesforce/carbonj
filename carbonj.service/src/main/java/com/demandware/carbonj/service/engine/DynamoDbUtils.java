/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.engine;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDbUtils {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbUtils.class);

    public static boolean isTablePresent(DynamoDbClient ddb, String tableName) {
        try {
            DescribeTableResponse resp = ddb.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            return "ACTIVE".equals(resp.table().tableStatusAsString());
        } catch (ResourceNotFoundException e) {
            log.warn("kinesis consumer table '{}' not found!", tableName);
            return false;
        }
    }

    public static void deleteTable(DynamoDbClient ddb, String tableName) throws InterruptedException {
        ddb.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        log.info("Waiting for {} to be deleted...this may take a while...", tableName);
        try (DynamoDbWaiter waiter = ddb.waiter()) {
            WaiterResponse<DescribeTableResponse> waiterResponse = waiter.waitUntilTableNotExists(
                    DescribeTableRequest.builder().tableName(tableName).build());
            waiterResponse.matched();
        }
    }
}
