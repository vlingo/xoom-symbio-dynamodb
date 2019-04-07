// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static java.util.Collections.singletonList;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreEntryReader;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.handlers.BatchWriteItemAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.GetEntityAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public class DynamoDBStateActor<RS extends State<?>> extends Actor implements StateStore {
    public static final String DISPATCHABLE_TABLE_NAME = "vlingo_dispatchables";
    private final StateStore.Dispatcher dispatcher;
    private final StateStore.DispatcherControl dispatcherControl;
    private final AmazonDynamoDBAsync dynamodb;
    private final CreateTableInterest createTableInterest;
    private final Map<String,StateStoreEntryReader<?>> entryReaders;
    private final EntryAdapterProvider entryAdapterProvider;
    private final StateAdapterProvider stateAdapterProvider;
    private final RecordAdapter<RS> recordAdapter;

    /**
     * NOTE: this constructor is intended <u>only</u> for supporting testing with mocks.
     *
     * @param dispatcher the {@link StateStore.Dispatcher} that will handle dispatching state changes
     * @param dispatcherControl the {@link StateStore.DispatcherControl} this will handle resipatching and dispatch confirmation
     * @param dynamodb the {@link AmazonDynamoDBAsync} that provide async access to Amazon DynamoDB
     * @param createTableInterest the {@link CreateTableInterest} that is responsible for table creation
     * @param recordAdapter the {@link RecordAdapter} that is responsible for un/marshalling state
     */
    public DynamoDBStateActor(
      StateStore.Dispatcher dispatcher,
      StateStore.DispatcherControl dispatcherControl,
      AmazonDynamoDBAsync dynamodb,
      CreateTableInterest createTableInterest,
      RecordAdapter<RS> recordAdapter)
    {
      this.dispatcher = dispatcher;
      this.dynamodb = dynamodb;
      this.createTableInterest = createTableInterest;
      this.recordAdapter = recordAdapter;
      this.dispatcherControl = dispatcherControl;
      this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
      this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
      this.entryReaders = new HashMap<>();

      createTableInterest.createDispatchableTable(dynamodb, DISPATCHABLE_TABLE_NAME);

      dispatcher.controlWith(dispatcherControl);
    }

    /**
     * Constructs a {@link DynamoDBStateActor} with the arguments.
     *
     * @param dispatcher the {@link StateStore.Dispatcher} that will handle dispatching state changes
     * @param dynamodb the {@link AmazonDynamoDBAsync} that provide async access to Amazon DynamoDB
     * @param createTableInterest the {@link CreateTableInterest} that is responsible for table creation
     * @param recordAdapter the {@link RecordAdapter} that is responsible for un/marshalling state
     */
    public DynamoDBStateActor(
      StateStore.Dispatcher dispatcher,
      AmazonDynamoDBAsync dynamodb,
      CreateTableInterest createTableInterest,
      RecordAdapter<RS> recordAdapter)
    {
        this.dispatcher = dispatcher;
        this.dynamodb = dynamodb;
        this.createTableInterest = createTableInterest;
        this.recordAdapter = recordAdapter;
        this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
        this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
        this.entryReaders = new HashMap<>();

        createTableInterest.createDispatchableTable(dynamodb, DISPATCHABLE_TABLE_NAME);

        this.dispatcherControl = stage().actorFor(
          DispatcherControl.class,
          Definition.has(
            DynamoDBDispatcherControlActor.class,
            Definition.parameters(dispatcher, dynamodb, recordAdapter, 1000L, 1000L)));

        dispatcher.controlWith(dispatcherControl);
    }

    @Override
    public void read(String id, Class<?> type, ReadResultInterest interest, Object object) {
      doGenericRead(id, type, interest, object);
    }


    @Override
    public <S,C> void write(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {
      doGenericWrite(id, state, stateVersion, sources, metadata, interest, object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ET extends Entry<?>> Completes<StateStoreEntryReader<ET>> entryReader(final String name) {
      StateStoreEntryReader<?> reader = entryReaders.get(name);
      if (reader == null) {
        reader = childActorFor(StateStoreEntryReader.class, Definition.has(DynamoDBStateStoreEntryReaderActor.class, Definition.parameters(name)));
        entryReaders.put(name, reader);
      }
      return completes().with((StateStoreEntryReader<ET>) reader);
    }

    protected final String tableFor(Class<?> type) {
        String tableName = "vlingo_" + type.getCanonicalName().replace(".", "_");
        StateTypeStateStoreMap.stateTypeToStoreName(type, tableName);
        return tableName;
    }

    private final void doGenericRead(String id, Class<?> type, StateStore.ReadResultInterest interest, final Object object) {
        dynamodb.getItemAsync(readRequestFor(id, type), new GetEntityAsyncHandler<>(id, interest, object, recordAdapter::unmarshallState, stateAdapterProvider));
    }

    private final <S,C> void doGenericWrite(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {
        String tableName = tableFor(state.getClass());
        createTableInterest.createEntityTable(dynamodb, tableName);
        final RS raw = metadata == null ?
                stateAdapterProvider.asRaw(id, state, stateVersion) :
                stateAdapterProvider.asRaw(id, state, stateVersion, metadata);

        try {
            Map<String, AttributeValue> foundItem = dynamodb.getItem(readRequestFor(id, state.getClass())).getItem();
            if (foundItem != null) {
                try {
                    RS savedState = recordAdapter.unmarshallState(foundItem);
                    if (savedState.dataVersion > raw.dataVersion) {
                        interest.writeResultedIn(Failure.of(new StorageException(Result.ConcurrentyViolation, "Concurrent modification of: " + id)), id, state, stateVersion, sources, object);
                        return;
                    }
                } catch (Exception e) {
                    interest.writeResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, state, stateVersion, sources, object);
                    return;
                }
            }
        } catch (Exception e) {
            // in case of error (for now) just try to write the record
        }

        // TODO: Write sources
        entryAdapterProvider.asEntries(sources); // final List<Entry<?>> entries =

        StateStore.Dispatchable<RS> dispatchable = new StateStore.Dispatchable<>(state.getClass().getName() + ":" + id, LocalDateTime.now(), raw);

        Map<String, List<WriteRequest>> transaction = writeRequestFor(raw, dispatchable);
        BatchWriteItemRequest request = new BatchWriteItemRequest(transaction);
        dynamodb.batchWriteItemAsync(request, new BatchWriteItemAsyncHandler<S,RS,C>(id, state, stateVersion, sources, interest, object, dispatchable, dispatcher, this::doDispatch));
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
