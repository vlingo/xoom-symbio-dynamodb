// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.*;
import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.*;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.StateStoreEntryReader;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBStateStoreEntryReaderActor.DynamoDBStateStoreEntryReaderInstantiator;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.handlers.BatchWriteItemAsyncHandler;
import io.vlingo.xoom.symbio.store.state.dynamodb.handlers.GetEntityAsyncHandler;
import io.vlingo.xoom.symbio.store.state.dynamodb.interests.CreateTableInterest;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class DynamoDBStateActor<RS extends State<?>> extends Actor implements StateStore {
    public static final String DISPATCHABLE_TABLE_NAME = "xoom_dispatchables";
    private final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers;
    private final AmazonDynamoDBAsync dynamodb;
    private final CreateTableInterest createTableInterest;
    private final Map<String,StateStoreEntryReader<?>> entryReaders;
    private final EntryAdapterProvider entryAdapterProvider;
    private final StateAdapterProvider stateAdapterProvider;
    private final RecordAdapter<RS> recordAdapter;

    /**
     * NOTE: this constructor is intended <u>only</u> for supporting testing with mocks.
     *
     * @param dispatchers the {@code List<Dispatcher<Dispatchable<Entry<?>, RS>>>} that will handle dispatching state changes
     * @param dispatcherControl the {@code DispatcherControl} this will handle re-dispatching and dispatch confirmation
     * @param dynamodb the {@code AmazonDynamoDBAsync} that provide async access to Amazon DynamoDB
     * @param createTableInterest the {@code CreateTableInterest} that is responsible for table creation
     * @param recordAdapter the {@code RecordAdapter} that is responsible for un/marshalling state
     */
    public DynamoDBStateActor(
      List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers,
      DispatcherControl dispatcherControl,
      AmazonDynamoDBAsync dynamodb,
      CreateTableInterest createTableInterest,
      RecordAdapter<RS> recordAdapter)
    {
      this.dispatchers = dispatchers;
      this.dynamodb = dynamodb;
      this.createTableInterest = createTableInterest;
      this.recordAdapter = recordAdapter;
      this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
      this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
      this.entryReaders = new HashMap<>();

      createTableInterest.createDispatchableTable(dynamodb, DISPATCHABLE_TABLE_NAME);

      dispatchers.forEach(d -> d.controlWith(dispatcherControl));
    }

    @Override
    public void read(String id, Class<?> type, ReadResultInterest interest, Object object) {
      doGenericRead(id, type, interest, object);
    }

    @Override
    public void readAll(final Collection<TypedStateBundle> bundles, final ReadResultInterest interest, final Object object) {
      // TODO: Implement
    }

    @Override
    public Completes<Stream> streamAllOf(final Class<?> stateType) {
      // TODO: Implement
      return null;
    }

    @Override
    public Completes<Stream> streamSomeUsing(final QueryExpression query) {
      // TODO: Implement
      return null;
    }

    @Override
    public <S,C> void write(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {
      doGenericWrite(id, state, stateVersion, sources, metadata, interest, object);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <ET extends Entry<?>> Completes<StateStoreEntryReader<ET>> entryReader(final String name) {
      StateStoreEntryReader<?> reader = entryReaders.get(name);
      if (reader == null) {
        reader = childActorFor(StateStoreEntryReader.class, Definition.has(DynamoDBStateStoreEntryReaderActor.class, new DynamoDBStateStoreEntryReaderInstantiator(name)));
        entryReaders.put(name, reader);
      }
      return completes().with((StateStoreEntryReader<ET>) reader);
    }

    protected final String tableFor(Class<?> type) {
        String tableName = "xoom_" + type.getCanonicalName().replace(".", "_");
        StateTypeStateStoreMap.stateTypeToStoreName(type, tableName);
        return tableName;
    }

    private void doGenericRead(String id, Class<?> type, StateStore.ReadResultInterest interest, final Object object) {
        dynamodb.getItemAsync(readRequestFor(id, type), new GetEntityAsyncHandler<>(id, interest, object, recordAdapter::unmarshallState, stateAdapterProvider));
    }

    private <S,C> void doGenericWrite(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {
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
                        interest.writeResultedIn(Failure.of(new StorageException(Result.ConcurrencyViolation, "Concurrent modification of: " + id)), id, state, stateVersion, sources, object);
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
        final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, stateVersion, metadata);// final List<Entry<?>> entries =

        Dispatchable<Entry<?>, RS> dispatchable = new Dispatchable<>(state.getClass().getName() + ":" + id, LocalDateTime.now(), raw, entries);

        Map<String, List<WriteRequest>> transaction = writeRequestFor(raw, dispatchable);
        BatchWriteItemRequest request = new BatchWriteItemRequest(transaction);
        dynamodb.batchWriteItemAsync(request, new BatchWriteItemAsyncHandler<>(id, state, stateVersion, sources, interest, object, dispatchable, dispatchers, this::doDispatch));
    }

    private GetItemRequest readRequestFor(String id, Class<?> type) {
        String table = tableFor(type);
        Map<String, AttributeValue> stateItem = recordAdapter.marshallForQuery(id);

        return new GetItemRequest(table, stateItem, true);
    }

    private Map<String, List<WriteRequest>> writeRequestFor(RS raw, Dispatchable<Entry<?>, RS> dispatchable) {
        Map<String, List<WriteRequest>> requests = new HashMap<>(2);

        requests.put(tableFor(raw.typed()),
                singletonList(new WriteRequest(new PutRequest(recordAdapter.marshallState(raw)))));

        requests.put(DISPATCHABLE_TABLE_NAME,
                singletonList(new WriteRequest(new PutRequest(recordAdapter.marshallDispatchable(dispatchable)))));

        return requests;
    }

    private Void doDispatch(Dispatchable<Entry<?>, RS> dispatchable) {
      dispatchers.forEach(d -> d.dispatch(dispatchable));
      return null;
    }

    public static class DynamoDBStateStoreInstantiator<RS extends State<?>> implements ActorInstantiator<DynamoDBStateActor<RS>> {
      private static final long serialVersionUID = -4104107435844928168L;

      private final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers;
      private final DispatcherControl dispatcherControl;
      private final AmazonDynamoDBAsync dynamodb;
      private final CreateTableInterest createTableInterest;
      private final RecordAdapter<RS> recordAdapter;

      public DynamoDBStateStoreInstantiator(
              final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers,
              final DispatcherControl dispatcherControl,
              final AmazonDynamoDBAsync dynamodb,
              final CreateTableInterest createTableInterest,
              final RecordAdapter<RS> recordAdapter) {
        this.dispatchers = dispatchers;
        this.dispatcherControl = dispatcherControl;
        this.dynamodb = dynamodb;
        this.createTableInterest = createTableInterest;
        this.recordAdapter = recordAdapter;
      }

      @Override
      public DynamoDBStateActor<RS> instantiate() {
        return new DynamoDBStateActor<>(dispatchers, dispatcherControl, dynamodb, createTableInterest, recordAdapter);
      }

      @Override
      @SuppressWarnings({ "unchecked", "rawtypes" })
      public Class<DynamoDBStateActor<RS>> type() {
        return (Class) DynamoDBStateActor.class;
      }
    }
}
