// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb.interests;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

public class NoopCreateTableInterest implements CreateTableInterest {
    @Override
    public void createDispatchableTable(AmazonDynamoDBAsync dynamoDBAsync, String tableName) {

    }

    @Override
    public void createEntityTable(AmazonDynamoDBAsync dynamoDBAsync, String tableName) {

    }
}
