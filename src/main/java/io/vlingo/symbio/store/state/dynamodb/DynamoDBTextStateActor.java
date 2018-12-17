// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.TextStateStore;
import io.vlingo.symbio.store.state.dynamodb.adapters.TextStateRecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public class DynamoDBTextStateActor extends DynamoDBStateActor<String> implements TextStateStore {
    public DynamoDBTextStateActor(Dispatcher dispatcher, AmazonDynamoDBAsync dynamodb, CreateTableInterest createTableInterest) {
        super(dispatcher, dynamodb, createTableInterest, new TextStateRecordAdapter(), State.NullState.Text);

        createTableInterest.createDispatchableTable(dynamodb, DISPATCHABLE_TABLE_NAME);
    }

    @Override
    public void read(String id, Class<?> type, ReadResultInterest<String> interest) {
        doGenericRead(id, type, interest, null);
    }

    @Override
    public void read(String id, Class<?> type, ReadResultInterest<String> interest, final Object object) {
        doGenericRead(id, type, interest, object);
    }

    @Override
    public void write(State<String> state, WriteResultInterest<String> interest) {
        doGenericWrite(state, interest, null);
    }

    @Override
    public void write(State<String> state, WriteResultInterest<String> interest, final Object object) {
        doGenericWrite(state, interest, object);
    }

    @Override
    protected Void doDispatch(Dispatchable<String> dispatchable) {
        dispatcher.dispatch(dispatchable.id, dispatchable.state.asTextState());
        return null;
    }
}
