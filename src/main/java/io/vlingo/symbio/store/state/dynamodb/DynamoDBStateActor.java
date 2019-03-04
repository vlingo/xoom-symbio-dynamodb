// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static java.util.Collections.singletonList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.actors.Actor;
import io.vlingo.common.Failure;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreAdapterAssistant;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.handlers.BatchWriteItemAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.ConfirmDispatchableAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.DispatchAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.GetEntityAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public class DynamoDBStateActor<RS extends State<?>> extends Actor implements StateStore, StateStore.DispatcherControl {
    public static final String DISPATCHABLE_TABLE_NAME = "vlingo_dispatchables";
    private final StateStoreAdapterAssistant adapterAssistant;
    private final StateStore.Dispatcher dispatcher;
    private final AmazonDynamoDBAsync dynamodb;
    private final CreateTableInterest createTableInterest;
    private final RecordAdapter<RS> recordAdapter;

    public DynamoDBStateActor(
            StateStore.Dispatcher dispatcher,
            AmazonDynamoDBAsync dynamodb,
            CreateTableInterest createTableInterest,
            RecordAdapter<RS> recordAdapter
    ) {
        this.dispatcher = dispatcher;
        this.dynamodb = dynamodb;
        this.createTableInterest = createTableInterest;
        this.recordAdapter = recordAdapter;
        this.adapterAssistant = new StateStoreAdapterAssistant();

        createTableInterest.createDispatchableTable(dynamodb, DISPATCHABLE_TABLE_NAME);
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

    @Override
    public void read(final String id, final Class<?> type, final ReadResultInterest interest) {
      read(id, type, interest, null);
    }

    @Override
    public void read(final String id, final Class<?> type, final ReadResultInterest interest, final Object object) {
      doGenericRead(id, type, interest, object);
    }

    @Override
    public <S> void write(final String id, final S state, final int stateVersion, final Metadata metadata, final WriteResultInterest interest) {
      write(id, state, stateVersion, metadata, interest, null);
    }

    @Override
    public <S> void write(final String id, final S state, final int stateVersion, final WriteResultInterest interest) {
      write(id, state, stateVersion, null, interest, null);
    }

    @Override
    public <S> void write(final String id, final S state, final int stateVersion, final WriteResultInterest interest, final Object object) {
      this.doGenericWrite(id, state, stateVersion, null, interest, object);
    }

    @Override
    public <S> void write(final String id, final S state, final int stateVersion, final Metadata metadata, final WriteResultInterest interest, final Object object) {
      this.doGenericWrite(id, state, stateVersion, metadata, interest, object);
    }

    @Override
    public <S, R extends State<?>> void registerAdapter(Class<S> stateType, StateAdapter<S, R> adapter) {
      adapterAssistant.registerAdapter(stateType, adapter);
    }

    protected final String tableFor(Class<?> type) {
        String tableName = "vlingo_" + type.getCanonicalName().replace(".", "_");
        StateTypeStateStoreMap.stateTypeToStoreName(type, tableName);
        return tableName;
    }

    private final void doGenericRead(String id, Class<?> type, StateStore.ReadResultInterest interest, final Object object) {
        dynamodb.getItemAsync(readRequestFor(id, type), new GetEntityAsyncHandler<>(id, interest, object, recordAdapter::unmarshallState, adapterAssistant));
    }

    private final <S> void doGenericWrite(final String id, final S state, final int stateVersion, final Metadata metadata, final WriteResultInterest interest, final Object object) {
        String tableName = tableFor(state.getClass());
        createTableInterest.createEntityTable(dynamodb, tableName);
        final RS raw = metadata == null ?
                adapterAssistant.adaptToRawState(state, stateVersion) :
                adapterAssistant.adaptToRawState(state, stateVersion, metadata);
        try {
            Map<String, AttributeValue> foundItem = dynamodb.getItem(readRequestFor(id, state.getClass())).getItem();
            if (foundItem != null) {
                try {
                    RS savedState = recordAdapter.unmarshallState(foundItem);
                    if (savedState.dataVersion > raw.dataVersion) {
                        interest.writeResultedIn(Failure.of(new StorageException(Result.ConcurrentyViolation, "Concurrent modification of: " + id)), id, state, stateVersion, object);
                        return;
                    }
                } catch (Exception e) {
                    interest.writeResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, state, stateVersion, object);
                    return;
                }
            }
        } catch (Exception e) {
            // in case of error (for now) just try to write the record
        }

        StateStore.Dispatchable<RS> dispatchable = new StateStore.Dispatchable<>(state.getClass().getName() + ":" + id, LocalDateTime.now(), raw);

        Map<String, List<WriteRequest>> transaction = writeRequestFor(raw, dispatchable);
        BatchWriteItemRequest request = new BatchWriteItemRequest(transaction);
        dynamodb.batchWriteItemAsync(request, new BatchWriteItemAsyncHandler<S,RS>(id, state, stateVersion, interest, object, dispatchable, dispatcher, this::doDispatch));
    }

    private GetItemRequest readRequestFor(String id, Class<?> type) {
        String table = tableFor(type);
        Map<String, AttributeValue> stateItem = recordAdapter.marshallForQuery(id);

        return new GetItemRequest(table, stateItem, true);
    }

    private Map<String, List<WriteRequest>> writeRequestFor(RS raw, StateStore.Dispatchable<RS> dispatchable) {
        Map<String, List<WriteRequest>> requests = new HashMap<>(2);

        requests.put(tableFor(raw.typed()),
                singletonList(new WriteRequest(new PutRequest(recordAdapter.marshallState(raw)))));

        requests.put(DISPATCHABLE_TABLE_NAME,
                singletonList(new WriteRequest(new PutRequest(recordAdapter.marshallDispatchable(dispatchable)))));

        return requests;
    }

    private Void doDispatch(Dispatchable<?> dispatchable) {
      dispatcher.dispatch(dispatchable.id, dispatchable.state);
      return null;
    }
}
