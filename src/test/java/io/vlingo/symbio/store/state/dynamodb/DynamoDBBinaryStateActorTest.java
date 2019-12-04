// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.Collections;

import org.junit.Before;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State.BinaryState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.Entity1.Entity1BinaryStateAdapter;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.dynamodb.DynamoDBDispatcherControlActor.DynamoDBDispatcherControlInstantiator;
import io.vlingo.symbio.store.state.dynamodb.DynamoDBStateActor.DynamoDBStateStoreInstantiator;
import io.vlingo.symbio.store.state.dynamodb.adapters.BinaryStateRecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

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
          new DynamoDBDispatcherControlInstantiator(dispatcher, dynamodb, new BinaryStateRecordAdapter(), 1000L, 1000L)));

      stateStore = stateStoreProtocol(world, dispatcher, dispatcherControl, dynamodb, createTableInterest);
    }

    @Override
    protected StateStore stateStoreProtocol(World world, Dispatcher<Dispatchable<Entry<?>, BinaryState>> dispatcher, DispatcherControl dispatcherControl, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
      return world.actorFor(
        StateStore.class,
        Definition.has(
                DynamoDBStateActor.class,
                new DynamoDBStateStoreInstantiator<>(dispatcher, dispatcherControl, dynamodb, interest, new BinaryStateRecordAdapter()))
      );
    }

    @Override
    protected void verifyDispatched(Dispatcher<Dispatchable<Entry<?>, BinaryState>> dispatcher, String id, Dispatchable<Entry<?>,BinaryState> dispatchable) {
        verify(dispatcher).dispatch(dispatchable);
    }

    @Override
    protected void verifyDispatched(Dispatcher<Dispatchable<Entry<?>, BinaryState>> dispatcher, String id, BinaryState state) {
        verify(dispatcher, timeout(DEFAULT_TIMEOUT)).dispatch(new Dispatchable<>(id, LocalDateTime.now(), state, Collections.emptyList()));
    }

    @Override
    protected RecordAdapter<BinaryState> recordAdapter() {
        return new BinaryStateRecordAdapter();
    }
}