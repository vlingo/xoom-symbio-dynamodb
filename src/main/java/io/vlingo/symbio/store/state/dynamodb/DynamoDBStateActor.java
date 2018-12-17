// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static java.util.Collections.singletonList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import io.vlingo.actors.Actor;
import io.vlingo.common.Failure;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.handlers.BatchWriteItemAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.ConfirmDispatchableAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.DispatchAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.GetEntityAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public abstract class DynamoDBStateActor<T> extends Actor implements StateStore.DispatcherControl {
    public static final String DISPATCHABLE_TABLE_NAME = "vlingo_dispatchables";
    protected final StateStore.Dispatcher dispatcher;
    protected final AmazonDynamoDBAsync dynamodb;
    protected final CreateTableInterest createTableInterest;
    protected final RecordAdapter<T> recordAdapter;
    protected final State<T> nullState;

    public DynamoDBStateActor(
            StateStore.Dispatcher dispatcher,
            AmazonDynamoDBAsync dynamodb,
            CreateTableInterest createTableInterest,
            RecordAdapter<T> recordAdapter,
            State<T> nullState
    ) {
        this.dispatcher = dispatcher;
        this.dynamodb = dynamodb;
        this.createTableInterest = createTableInterest;
        this.recordAdapter = recordAdapter;
        this.nullState = nullState;
    }

    @Override
    public void confirmDispatched(String dispatchId, StateStore.ConfirmDispatchedResultInterest interest) {
        dynamodb.deleteItemAsync(
                new DeleteItemRequest(
                        DISPATCHABLE_TABLE_NAME,
                        recordAdapter.marshallForQuery(dispatchId)),
                new ConfirmDispatchableAsyncHandler(dispatchId, interest)
        );
    }

    @Override
    public void dispatchUnconfirmed() {
        dynamodb.scanAsync(new ScanRequest(DISPATCHABLE_TABLE_NAME).withLimit(100), new DispatchAsyncHandler<>(recordAdapter::unmarshallDispatchable, this::doDispatch));
    }

    protected abstract Void doDispatch(StateStore.Dispatchable<T> dispatchable);

    protected final String tableFor(Class<?> type) {
        String tableName = "vlingo_" + type.getCanonicalName().replace(".", "_");
        StateTypeStateStoreMap.stateTypeToStoreName(type, tableName);
        return tableName;
    }

    protected final void doGenericRead(String id, Class<?> type, StateStore.ReadResultInterest<T> interest, final Object object) {
        dynamodb.getItemAsync(readRequestFor(id, type), new GetEntityAsyncHandler<T>(id, interest, object, nullState, recordAdapter::unmarshallState));
    }

    protected final void doGenericWrite(State<T> state, StateStore.WriteResultInterest<T> interest, final Object object) {
        String tableName = tableFor(state.typed());
        createTableInterest.createEntityTable(dynamodb, tableName);

        try {
            Map<String, AttributeValue> foundItem = dynamodb.getItem(readRequestFor(state.id, state.typed())).getItem();
            if (foundItem != null) {
                try {
                    State<T> savedState = recordAdapter.unmarshallState(foundItem);
                    if (savedState.dataVersion > state.dataVersion) {
                        interest.writeResultedIn(Failure.of(new StorageException(Result.ConcurrentyViolation, "Concurrent modification of: " + state.id)), state.id, savedState, object);
                        return;
                    }
                } catch (Exception e) {
                    interest.writeResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), state.id, state, object);
                    return;
                }
            }
        } catch (Exception e) {
            // in case of error (for now) just try to write the record
        }

        StateStore.Dispatchable<T> dispatchable = new StateStore.Dispatchable<>(state.type + ":" + state.id, state);

        Map<String, List<WriteRequest>> transaction = writeRequestFor(state, dispatchable);
        BatchWriteItemRequest request = new BatchWriteItemRequest(transaction);
        dynamodb.batchWriteItemAsync(request, new BatchWriteItemAsyncHandler<>(state, interest, object, dispatchable, dispatcher, nullState, this::doDispatch));
    }

    protected GetItemRequest readRequestFor(String id, Class<?> type) {
        String table = tableFor(type);
        Map<String, AttributeValue> stateItem = recordAdapter.marshallForQuery(id);

        return new GetItemRequest(table, stateItem, true);
    }

    protected Map<String, List<WriteRequest>> writeRequestFor(State<T> state, StateStore.Dispatchable<T> dispatchable) {
        Map<String, List<WriteRequest>> requests = new HashMap<>(2);

        requests.put(tableFor(state.typed()),
                singletonList(new WriteRequest(new PutRequest(recordAdapter.marshallState(state)))));

        requests.put(DISPATCHABLE_TABLE_NAME,
                singletonList(new WriteRequest(new PutRequest(recordAdapter.marshallDispatchable(dispatchable)))));

        return requests;
    }

}
