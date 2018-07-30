// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.interests;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

public interface CreateTableInterest {
    void createDispatchableTable(AmazonDynamoDBAsync dynamoDBAsync, String tableName);

    void createEntityTable(AmazonDynamoDBAsync dynamoDBAsync, String tableName);
}
