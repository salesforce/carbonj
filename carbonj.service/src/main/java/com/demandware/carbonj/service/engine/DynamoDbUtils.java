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
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DynamoDbUtils {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbUtils.class);

    public static boolean isTablePresent(DynamoDbAsyncClient client, String tableName, int checkPointDynamodbTimout) throws InterruptedException {
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();

        try {
            DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest)
                    .get(checkPointDynamodbTimout, TimeUnit.SECONDS);
            TableDescription tableDescription = describeTableResponse.table();
            return tableDescription.tableStatus() == TableStatus.ACTIVE;
        } catch (ExecutionException | TimeoutException e) {
            log.warn("kinesis consumer table '" + tableName + "' not found!");
            return false;
        }
    }
}
