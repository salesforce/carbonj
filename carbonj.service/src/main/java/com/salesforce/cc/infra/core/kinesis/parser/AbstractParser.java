/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.cc.infra.core.kinesis.parser;

abstract class AbstractParser implements Parser {
    static final String PROTOCOL_NAME = "UMONTP";

    final String versionLine;

    AbstractParser(String version) {
        this.versionLine = generateVersionLine(version);
    }

    private static String generateVersionLine(String version) {
        return PROTOCOL_NAME + "/" + version;
    }
}
