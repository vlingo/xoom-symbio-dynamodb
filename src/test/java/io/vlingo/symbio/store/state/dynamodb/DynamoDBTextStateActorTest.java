// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

import org.junit.Before;

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.Entity1.Entity1TextStateAdapter;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStore.Dispatcher;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.adapters.TextStateRecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public class DynamoDBTextStateActorTest extends DynamoDBStateActorTest<TextState> {
  
    @Before
    public void setUp() {
      super.setUp();

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
          Definition.parameters(dispatcher, dynamodb, new TextStateRecordAdapter(), 1000L, 1000L)));
      
      stateStore = stateStoreProtocol(world, dispatcher, dispatcherControl, dynamodb, createTableInterest);
      
      final Entity1TextStateAdapter adapter = new Entity1TextStateAdapter();
      stateStore.registerAdapter(Entity1.class, adapter);
      adapterAssistant.registerAdapter(Entity1.class, adapter);
    }

    @Override
    protected StateStore stateStoreProtocol(World world, Dispatcher dispatcher, DispatcherControl dispatcherControl, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
      return world.actorFor(
        StateStore.class,
        Definition.has(DynamoDBStateActor.class, Definition.parameters(dispatcher, dispatcherControl, dynamodb, interest, new TextStateRecordAdapter()))
      );
    }

    @Override
    protected void verifyDispatched(StateStore.Dispatcher dispatcher, String id, StateStore.Dispatchable<TextState> dispatchable) {
        verify(dispatcher).dispatch(dispatchable.id, dispatchable.state.asTextState());
    }

    @Override
    protected void verifyDispatched(StateStore.Dispatcher dispatcher, String id, TextState state) {
        verify(dispatcher, timeout(DEFAULT_TIMEOUT)).dispatch(id, state.asTextState());
    }

    @Override
    protected RecordAdapter<TextState> recordAdapter() {
        return new TextStateRecordAdapter();
    }
}