// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State.BinaryState;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.Entity1;
import io.vlingo.xoom.symbio.store.state.Entity1.Entity1BinaryStateAdapter;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBDispatcherControlActor.DynamoDBDispatcherControlInstantiator;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBStateActor.DynamoDBStateStoreInstantiator;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.BinaryStateRecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.interests.CreateTableInterest;
import org.junit.Before;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class DynamoDBBinaryStateActorTest extends DynamoDBStateActorTest<BinaryState> {

    @Before
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setUp() {
      isBinaryTest = true;

      super.setUp();

      final StateAdapterProvider stateAdapterProvider = new StateAdapterProvider(world);
      stateAdapterProvider.registerAdapter(Entity1.class, new Entity1BinaryStateAdapter());

      /*
       * NOTE: dispatcherControl is only created here and passed to stateStoreProtocol
       * so that this test case can directly interact with dispatcherControl. Normally
       * the DynamoDBStateActor provisions its own DispatcherControl (an instance of
       * DynamoDBDispatcherControlActor).
       */
      dispatcherControl = world.actorFor(
        DispatcherControl.class,
        Definition.has(
          DynamoDBDispatcherControlActor.class,
          new DynamoDBDispatcherControlInstantiator(dispatchers, dynamodb, new BinaryStateRecordAdapter(), 1000L, 1000L)));

      stateStore = stateStoreProtocol(world, dispatchers, dispatcherControl, dynamodb, createTableInterest);
    }

    @Override
    protected StateStore stateStoreProtocol(World world, List<Dispatcher<Dispatchable<Entry<?>, BinaryState>>> dispatchers, DispatcherControl dispatcherControl, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
      return world.actorFor(
        StateStore.class,
        Definition.has(
                DynamoDBStateActor.class,
                new DynamoDBStateStoreInstantiator<>(dispatchers, dispatcherControl, dynamodb, interest, new BinaryStateRecordAdapter()))
      );
    }

    @Override
    protected void verifyDispatched(List<Dispatcher<Dispatchable<Entry<?>, BinaryState>>> dispatchers, String id, Dispatchable<Entry<?>,BinaryState> dispatchable) {
      dispatchers.forEach(dispatcher -> verify(dispatcher, atLeast(1)).dispatch(dispatchable));
    }

    @Override
    protected void verifyDispatched(List<Dispatcher<Dispatchable<Entry<?>, BinaryState>>> dispatchers, String id, BinaryState state) {
      dispatchers.forEach(dispatcher -> verify(dispatcher, timeout(DEFAULT_TIMEOUT).atLeast(1)).dispatch(new Dispatchable<>(id, LocalDateTime.now(), state, Collections.emptyList())));
    }

    @Override
    protected RecordAdapter<BinaryState> recordAdapter() {
        return new BinaryStateRecordAdapter();
    }
}